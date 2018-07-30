package com.jd.bgn.rule_engine

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SaveMode, Dataset}
import org.apache.spark.sql.functions._
import scala.util.matching.Regex._

import com.jd.bgn.rules.{Common, Matching, Range}
import com.jd.bgn.defenitions.{Config, Source, Target}
import com.jd.bgn.utils.AttrSetFileIO

class RuleEngine(
  _config: Config,
  _attr_group_map: Map[String, Map[String, String]],
  _common_rules: Map[String, Map[String, Common]],
  _matching_rules: Map[String, Map[String, Matching]],
  _range_rules: Map[String, Map[String, Range]]
) extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private final var config: Config = null
  private final var attr_group_map: Map[String, Map[String, String]] = null
  private final var common_rules: Map[String, Map[String, Common]] = null
  private final var matching_rules: Map[String, Map[String, Matching]] = null
  private final var range_rules: Map[String, Map[String, Range]] = null

  def run(): Unit = {
    val spark = SparkEnv.getSession
    val sc = spark.sparkContext
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val config_broadcast = sc.broadcast(_config)
    config = config_broadcast.value
    val attr_group_map_broadcast = sc.broadcast(_attr_group_map)
    attr_group_map = attr_group_map_broadcast.value
    val common_rules_broadcast = sc.broadcast(_common_rules)
    common_rules = common_rules_broadcast.value
    val matching_rules_broadcast = sc.broadcast(_matching_rules)
    matching_rules = matching_rules_broadcast.value
    val range_rules_broadcast = sc.broadcast(_range_rules)
    range_rules = range_rules_broadcast.value

    val source = getSource()
    logger.info(s"count of source data is: ${source.count}")
    val (train_source, inference_source) = splitSource(source)
    logger.info(s"count of train source data is: ${train_source.count}")
    logger.info(s"count of inference source data is: ${inference_source.count}")
    var attr_set = null
    if (config.use_local_attr_set) {
      attr_set = new AttrSetFileIO().read("./.ads_bgn_attr_recovery_attr_set")
    } else {
      attr_set = getAttrSet(train_source)
      new AttrSetFileIO().write(attr_set, "./.ads_bgn_attr_recovery_attr_set")
    }
    logger.info(s"size of attr set is: ${attr_set.size}")
    val attr_set_broadcast = sc.broadcast(attr_set)
    // debug start
    attr_set.foreach((e) => {
      println("key:", e._1)
      e._2.foreach((se) => {
        println("                ", se._1, se._2, se._3)
      })
    })
    // debug end

    val required_attr = getRequiredAttr(train_source)
    val required_attr_broadcast = sc.broadcast(required_attr)
    // debug start
    required_attr.foreach((e) => {
      println("key:", e._1)
      e._2.foreach(println("       ", _))
    })
    // debug end

    val supplement = getSupplement(source, required_attr_broadcast.value)
    logger.info(s"count of supplement is: ${supplement.count}")

    val under_rules = assembleUnderRules(inference_source, supplement, train_source)

    val target = execRules(under_rules, attr_set_broadcast.value)
    logger.info(s"count of target is: ${target.count}")

    writeTable(target)
    spark.close()
  }
  private def writeTable(target: Dataset[Target]): Unit = {
    val spark = SparkEnv.getSession
    import spark.implicits._
    target.select(
            col("item_first_cate_cd"),
            col("item_second_cate_cd"),
            col("item_third_cate_cd"),
            col("item_sku_id"),
            col("sku_name"),
            col("barndname_full"),
            col("jd_prc"),
            col("com_attr_cd"),
            col("com_attr_name"),
            col("com_attr_value_cd"),
            col("com_attr_value_name"),
            col("old_attr_value_cd"),
            col("old_attr_value_name"),
            col("flag"),
            lit(config.source_id) alias "source",
            lit(config.date) alias "dt")
          .write
          .mode(SaveMode.Append)
          .insertInto(config.target_db_table)
  }
  private def execRules(under_rules: Dataset[Target], attr_set: Map[(String, String, String, String), Array[(String, String, Int, Array[String])]]): Dataset[Target] = {
    val target = under_rules.map((row) => {
                              val revision = if (row.alt_attr_value_cd != null && row.alt_attr_value_cd != ""
                                                 && row.alt_attr_value_name != null && row.alt_attr_value_name != "") {
                                (row.alt_attr_value_cd, row.alt_attr_value_name)
                              } else {
                                val rule = common_rules(row.item_first_cate_cd)(row.com_attr_group).rule
                                val columns = common_rules(row.item_first_cate_cd)(row.com_attr_group).columns
                                val metas = columns.map(row.getMeta(_))
                                val key = common_rules(row.item_first_cate_cd)(row.com_attr_group).getCateKey(
                                  row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd)
                                rule match {
                                  case "matching" => matching_rules(row.item_first_cate_cd)(row.com_attr_group).process(metas, attr_set((key._1, key._2, key._3, row.com_attr_group)))
                                  case "range" => range_rules(row.item_first_cate_cd)(row.com_attr_group).process(metas, attr_set((key._1, key._2, key._3, row.com_attr_group)))
                                  case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${row.item_first_cate_cd} and com_attr_group ${row.com_attr_group}")
                                }
                              }
                              row.copy(old_attr_value_cd = row.com_attr_value_cd, old_attr_value_name = row.com_attr_value_name,
                                       com_attr_value_cd = revision._1, com_attr_value_name = revision._2)
                            })
                            .cache()
    target
  }
  private def assembleUnderRules(inference_source: Dataset[Source], supplement: Dataset[Source], train_source: Dataset[Source]): Dataset[Target] = {
    val temp = inference_source.withColumn("flag", lit("revision"))
                               .union(supplement.withColumn("flag", lit("supplement")))
    var under_rules = temp.join(train_source, under_rules("item_first_cate_cd") === train_source("item_first_cate_cd") and
                                under_rules("item_second_cate_cd") === train_source("item_second_cate_cd") and
                                under_rules("item_third_cate_cd") === train_source("item_third_cate_cd") and
                                under_rules("item_sku_id") === train_source("item_sku_id") and
                                under_rules("com_attr_group") === train_source("com_attr_group"), "left")
                          .select(temp("item_first_cate_cd"), temp("item_second_cate_cd"), temp("item_third_cate_cd"),
                                  temp("item_sku_id"), temp("sku_name"), temp("barndname_full"), temp("jd_prc"),
                                  temp("com_attr_cd"), temp("com_attr_name"), temp("com_attr_value_cd"),
                                  temp("com_attr_value_name"), temp("com_attr_group"), temp("flag"))
                          .withColumn("alt_attr_value_cd", train_source("com_attr_value_cd"))
                          .withColumn("alt_attr_value_name", train_source("com_attr_value_name"))
                          .withColumn("old_attr_value_cd", lit(null))
                          .withColumn("old_attr_value_name", lit(null))
                          .as[Target]
    under_rules
  }
  private def getAttrSet(train_source: Dataset[Source]): Map[(String, String, String, String), Array[(String, String, Int, Array[String])]] = {
    val attr_set = source.map((row) => {
                           val key = common_rules(row.item_first_cate_cd)(row.com_attr_group).getCateKey(
                                                  row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd)
                           row.copy(item_first_cate_cd = key._1, item_second_cate_cd = key._2, item_third_cate_cd = key._3)
                         })
                         .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"item_third_cate_cd",
                                  $"com_attr_group", $"com_attr_value_cd", $"com_attr_value_name")
                         .agg(countDistinct($"item_sku_id").as("com_attr_value_count"))
                         .withColumn("com_attr_value_preprocess", udf((item_first_cate_cd: String, com_attr_group: String, com_attr_value_name: String) => {
                           val rule = common_rules(item_first_cate_cd)(com_attr_group).rule
                           rule match {
                             case "matching" => matching_rules(item_first_cate_cd)(com_attr_group).preprocess(com_attr_value_name)
                             case "range" => range_rules(item_first_cate_cd)(com_attr_group).preprocess(com_attr_value_name)
                             case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${item_first_cate_cd} and com_attr_group ${com_attr_group}")
                           }
                         })($"item_first_cate_cd", $"com_attr_group", $"com_attr_value_name"))
                         .collect()
                         .groupBy((row) => (row.getAs[String]("item_first_cate_cd"), row.getAs[String]("item_second_cate_cd"),
                                            row.getAs[String]("item_third_cate_cd"), row.getAs[String]("com_attr_group")))
                         .map {
                           case (k, v) => {
                             val rule = common_rules(k._1)(k._4)
                             val sorted_v = v.map((e) => (e.getAs[String]("com_attr_value_cd"), e.getAs[String]("com_attr_value_name")
                                                          e.getAs[Int]("com_attr_value_count"), e.getAs[Array[String]]("com_attr_value_preprocess")))
                                             .sortWith((value_a, value_b) => {
                                               rule.sort((value_a._1, value_a._2, value_a._3), (value_b._1, value_b._2, value_b._3))
                                             })
                             (k, sorted_v)
                           }
                         }
    attr_set
  }
  private def getSource(): Dataset[Source] = {
    val spark = SparkEnv.getSession
    import spark.implicits._
    val item_first_cate_cds = attr_group_map.keys
                                            .map("'" + _ + "'")
                                            .mkString("(", "," ,")")
    val com_attr_cds = attr_group_map.values
                                     .reduce(_ ++ _)
                                     .keys
                                     .map("'" + _ + "'")
                                     .mkString("(", "," ,")")
    val source_sku_da = spark.sql(
      s"""
        |select item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,item_sku_id,sku_name,barndname_full
        |  from gdm.gdm_m03_item_sku_da
        |  where item_first_cate_cd in ${item_first_cate_cds} and dt='${config.date}' and
        |  sku_valid_flag=1 and sku_status_cd!='3000' and sku_status_cd!='3010' and item_sku_id is not null and sku_name is not null
      """.stripMargin)
    val source_sku_da_mkt = spark.sql(
      s"""
        |select item_sku_id,jd_prc
        |  from gdm.gdm_m03_mkt_item_sku_da
        |  where dt='${config.date}'
      """.stripMargin)
    val source_sku_meta = source_sku_da.join(source_sku_da_mkt, source_sku_da("item_sku_id") === source_sku_da_mkt("item_sku_id"), "left")
                                       .groupBy(source_sku_da("item_first_cate_cd"), source_sku_da("item_second_cate_cd"), source_sku_da("item_third_cate_cd"),
                                                source_sku_da("item_sku_id"), source_sku_da("sku_name"), source_sku_da("barndname_full"))
                                       .agg(max(source_sku_da_mkt("jd_prc")).alias("jd_prc"))
    val source_attr = spark.sql(
      s"""
        |select item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name
        |  from gdm.gdm_m03_item_sku_ext_attr_da
        |  where dt='${config.date}' and com_attr_cd in ${com_attr_cds} and com_attr_name is not null
        |  union distinct
        |  select item_sku_id,com_attr_cd,com_attr_name,com_attr_value_cd,com_attr_value_name
        |  from gdm.gdm_m03_item_sku_spec_par_da
        |  where dt='${config.date}' and com_attr_cd in ${com_attr_cds} and com_attr_name is not null
      """.stripMargin)
    val toAttrGroup = udf((item_first_cate_cd: String, com_attr_cd: String) =>
      attr_group_map(item_first_cate_cd)(com_attr_cd))
    val filterCondition = udf((item_first_cate_cd: String, com_attr_cd: String) =>
      attr_group_map.contains(item_first_cate_cd) && attr_group_map(item_first_cate_cd).contains(com_attr_cd))
    val source = source_sku_meta.join(source_attr, source_sku_meta("item_sku_id") === source_attr("item_sku_id"))
                                .select(source_sku_meta("item_first_cate_cd"), source_sku_meta("item_second_cate_cd"),
                                        source_sku_meta("item_third_cate_cd"), source_sku_meta("item_sku_id"),
                                        source_sku_meta("sku_name"), source_sku_meta("barndname_full"),
                                        source_sku_meta("jd_prc"), source_attr("com_attr_cd"),
                                        source_attr("com_attr_name"), source_attr("com_attr_value_cd"), source_attr("com_attr_value_name"))
                                .filter(filterCondition($"item_first_cate_cd", $"com_attr_cd"))
                                .withColumn("com_attr_group", toAttrGroup($"item_first_cate_cd", $"com_attr_cd"))
                                .as(Source)
                                .cache()
    source
  }
  private def splitSource(source: Dataset[Source]): (Dataset[Source], Dataset[Source]) = {
    val train_filter = udf((item_first_cate_cd, com_attr_group, com_attr_value_cd, com_attr_value_name) =>
      common_rules(item_first_cate_cd)(com_attr_group).filter(com_attr_value_cd, com_attr_value_name)
    )
    val inference_filter = udf((item_first_cate_cd, com_attr_group, com_attr_value_cd, com_attr_value_name) =>
      !common_rules(item_first_cate_cd)(com_attr_group).filter(com_attr_value_cd, com_attr_value_name)
    )
    val train_source = source.filter(train_filter($"item_first_cate_cd", $"com_attr_group", $"com_attr_value_cd", $"com_attr_value_name"))
                             .cache()
    val inference_source = source.filter(inference_filter($"item_first_cate_cd", $"com_attr_group", $"com_attr_value_cd", $"com_attr_value_name"))
                                 .cache()
    (train_source, inference_source)
  }
  private def getRequiredAttr(train_source: Dataset[Source]): Map[(String, String), Set[(String, String)]] = {
    val cate_count = train_source.select($"item_first_cate_cd", $"item_second_cate_cd", $"item_sku_id")
                                 .groupBy($"item_first_cate_cd", $"item_second_cate_cd")
                                 .agg(countDistinct($"item_sku_id").as("cate_count"))
    val attr_count = train_source.select($"item_first_cate_cd", $"item_second_cate_cd",
                                         $"item_sku_id", $"com_attr_cd", $"com_attr_name")
                                 .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"com_attr_cd", $"com_attr_name")
                                 .agg(countDistinct($"item_sku_id").as("attr_count"))
    val required_attr = attr_count.join(cate_count, attr_count("item_first_cate_cd") === cate_count("item_first_cate_cd") and
                                        attr_count("item_second_cate_cd") === cate_count("item_second_cate_cd"))
                                  .select(attr_count("item_first_cate_cd"), attr_count("item_second_cate_cd"),
                                          attr_count("com_attr_cd"), attr_count("com_attr_name"))
                                  .withColumn("fraction", attr_count("attr_count") / cate_count("cate_count").cast("double"))
                                  .filter($"fraction" >= 0.6)
                                  .collect()
                                  .groupBy((row) => (row.getAs[String]("item_first_cate_cd"), row.getAs[String]("item_second_cate_cd")))
                                  .map {case (k, v) => (k, v.map((e) => (e.getAs[String]("com_attr_cd"), e.getAs[String]("com_attr_name"))).toSet)}
                                  .toMap
    required_attr
  }
  private def getSupplement(source: Dataset[Source], required_attr: Map[(String, String), Set[(String, String)]]): Dataset[Source] = {
    val supplement = source.groupByKey((row) => (row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd,
                                                 row.item_sku_id, row.sku_name, row.barndname_full, row.jd_prc))
                           .flatMapGroups((k, v) => {
                             val exist_attr = v.map((e) => (e.com_attr_cd, e.com_attr_name)).toSet
                             val supplement_attr = required_attr((k._1, k._2)) -- exist_attr
                             supplement_attr.map((e) => {
                               Source(k._1, k._2, k._3, k._4, k._5, k._6, k._7, e._1, e._2, null, null, attr_group_map(k._1)(e._1))
                             })
                           })
                           .cache()
    supplement
  }
}