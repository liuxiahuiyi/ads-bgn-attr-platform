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
  _common_rules: Map[(String, String), Common],
  _matching_rules: Map[(String, String), Matching],
  _range_rules: Map[(String, String), Range]
) extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private final var config: Config = null
  private final var attr_group_map: Map[String, Map[String, String]] = null
  private final var common_rules: Map[(String, String), Common] = null
  private final var matching_rules: Map[(String, String), Matching] = null
  private final var range_rules: Map[(String, String), Range] = null
  private final val spark = SparkEnv.getSession

  def run(): Unit = {
    val sc = spark.sparkContext
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val config_broadcast = sc.broadcast(_config)
    config = config_broadcast.value
    val attr_group_map_broadcast = sc.broadcast(_attr_group_map)
    attr_group_map = attr_group_map_broadcast.value
    // debug
    _attr_group_map.foreach((e) => {
      println(e._1)
      e._2.foreach(println("attr_group_map:", _))
    })
    // debug
    val common_rules_broadcast = sc.broadcast(_common_rules)
    common_rules = common_rules_broadcast.value
    val matching_rules_broadcast = sc.broadcast(_matching_rules)
    matching_rules = matching_rules_broadcast.value
    val range_rules_broadcast = sc.broadcast(_range_rules)
    range_rules = range_rules_broadcast.value

    val source = getSource()
    logger.info(s"count of source data is: ${source.count}")
    logger.info(s"schema of source data is: ${source.schema}")
    val attr_cd_name_map = getAttrCdNameMap(source)
    val attr_cd_name_map_broadcast = sc.broadcast(attr_cd_name_map)
    // debug start
    attr_cd_name_map.foreach((e) => {
      println("item_first_cate_cd:", e._1)
      e._2.foreach(println("attr_cd_name_map:", _))
    })
    // debug end

    val required_attr = getRequiredAttr(source)
    val required_attr_broadcast = sc.broadcast(required_attr)
    // debug start
    required_attr.foreach((e) => {
      println("item_first_cate_cd and item_second_cate_cd:", e._1)
      e._2.foreach(println("attr cd and name:", _))
    })
    // debug end
    val supplement = getSupplement(source, required_attr_broadcast.value)
    logger.info(s"count of supplement is: ${supplement.count}")
    logger.info(s"schema of supplement is: ${supplement.schema}")

    val valid_source = getValidSource(source)
    logger.info(s"count of valid source data is: ${valid_source.count}")
    logger.info(s"schema of valid source data is: ${valid_source.schema}")

    val revision = getRevision(source)
    logger.info(s"count of revision data is: ${revision.count}")
    logger.info(s"schema of revision data is: ${revision.schema}")

    val alternative = getAlternative(source)
    logger.info(s"count of alternative data is: ${alternative.count}")
    logger.info(s"schema of alternative data is: ${alternative.schema}")

    var attr_set: Map[(String, String, String, String), Array[(String, String, Long, Array[String])]] = null
    if (config.use_local_attr_set) {
      attr_set = new AttrSetFileIO().read("./.ads_bgn_attr_platform_rule_engine_attr_set")
    } else {
      attr_set = getAttrSet(valid_source)
      new AttrSetFileIO().write(attr_set, "./.ads_bgn_attr_platform_rule_engine_attr_set")
    }
    logger.info(s"size of attr set is: ${attr_set.size}")
    val attr_set_broadcast = sc.broadcast(attr_set)
    // debug start
    attr_set.foreach((e) => {
      println("cate_and_attr_group:", e._1)
      e._2.foreach((se) => {
        println("attr_values_sorted:", se._1, se._2, se._3)
      })
    })
    // debug end

    val revision_under_rules = assembleUnderRules(revision, alternative, "revision")
    logger.info(s"count of revision_under_rules is: ${revision_under_rules.count}")
    logger.info(s"schema of revision_under_rules is: ${revision_under_rules.schema}")
    val supplement_under_rules = assembleUnderRules(supplement, alternative, "supplement")
    logger.info(s"count of supplement_under_rules is: ${supplement_under_rules.count}")
    logger.info(s"schema of supplement_under_rules is: ${supplement_under_rules.schema}")

    val revision_target = execRules(revision_under_rules, attr_set_broadcast.value)
    logger.info(s"count of revision_target is: ${revision_target.count}")
    logger.info(s"schema of revision_target is: ${revision_target.schema}")
    val supplement_target = execRules(supplement_under_rules, attr_set_broadcast.value)
    logger.info(s"count of supplement_target is: ${supplement_target.count}")
    logger.info(s"schema of supplement_target is: ${supplement_target.schema}")

    writeTable(revision_target)
    writeTable(supplement_target)
    spark.close()
  }
  private def writeTable(target: Dataset[Target]): Unit = {
    import spark.implicits._
    target.select(
            col("item_first_cate_cd"),
            col("item_second_cate_cd"),
            col("item_third_cate_cd"),
            col("item_sku_id"),
            col("sku_name"),
            col("barndname_full"),
            col("colour"),
            col("size"),
            col("jd_prc"),
            col("com_attr_cd"),
            col("com_attr_name"),
            col("com_attr_value_cd"),
            col("com_attr_value_name"),
            col("com_attr_group"),
            col("flag"),
            col("alt_attr_value_cd"),
            col("alt_attr_value_name"),
            col("old_attr_value_cd"),
            col("old_attr_value_name"),
            lit(config.source_id) alias "source",
            lit(config.date) alias "dt")
          .write
          .mode(SaveMode.Append)
          .insertInto(config.target_db_table)
  }
  private def execRules(under_rules: Dataset[Target], attr_set: Map[(String, String, String, String), Array[(String, String, Long, Array[String])]]): Dataset[Target] = {
    import spark.implicits._
    val target = under_rules.map((row) => {
                              val revision = if (row.alt_attr_value_cd != null && row.alt_attr_value_cd != ""
                                                 && row.alt_attr_value_name != null && row.alt_attr_value_name != "") {
                                (row.alt_attr_value_cd, row.alt_attr_value_name)
                              } else {
                                val rule = common_rules((row.item_first_cate_cd, row.com_attr_group)).getRule
                                val columns = common_rules((row.item_first_cate_cd, row.com_attr_group)).getColumns
                                val metas = columns.map(row.getMeta(_))
                                val key = common_rules((row.item_first_cate_cd, row.com_attr_group)).getCateKey(
                                  row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd)
                                if (!attr_set.contains(key._1, key._2, key._3, row.com_attr_group)) {
                                  (null, null)
                                } else {
                                  rule match {
                                    case "matching" => matching_rules((row.item_first_cate_cd, row.com_attr_group)).process(metas, attr_set((key._1, key._2, key._3, row.com_attr_group)))
                                    case "range" => range_rules((row.item_first_cate_cd, row.com_attr_group)).process(metas, attr_set((key._1, key._2, key._3, row.com_attr_group)))
                                    case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${row.item_first_cate_cd} and com_attr_group ${row.com_attr_group}")
                                  }
                                }
                              }
                              row.copy(old_attr_value_cd = row.com_attr_value_cd, old_attr_value_name = row.com_attr_value_name,
                                       com_attr_value_cd = revision._1, com_attr_value_name = revision._2)
                            })
                            .filter((row) => row.com_attr_value_cd != null && row.com_attr_value_name != null)
                            .cache()
    target
  }
  private def assembleUnderRules(source: Dataset[Source], alternative: Dataset[Source], flag: String): Dataset[Target] = {
    import spark.implicits._
    val temp = source.withColumn("flag", lit(flag))
    val alt = alternative.select($"item_sku_id", $"com_attr_group", $"com_attr_value_cd".as("alt_attr_value_cd"),
                                 $"com_attr_value_name".as("alt_attr_value_name"))
    val under_rules = temp.join(alt, Seq("item_sku_id", "com_attr_group"), "left")
                          .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"item_third_cate_cd",
                                   $"item_sku_id", $"sku_name", $"barndname_full", $"colour", $"size", $"jd_prc",
                                   $"com_attr_cd", $"com_attr_name", $"com_attr_value_cd",
                                   $"com_attr_value_name", $"com_attr_group", $"flag")
                          .agg(first($"alt_attr_value_cd").as("alt_attr_value_cd"), first($"alt_attr_value_name").as("alt_attr_value_name"))
                          .withColumn("old_attr_value_cd", lit(null))
                          .withColumn("old_attr_value_name", lit(null))
                          .as[Target]
                          .cache()
    under_rules
  }
  private def getAttrSet(valid_source: Dataset[Source]): Map[(String, String, String, String), Array[(String, String, Long, Array[String])]] = {
    import spark.implicits._
    val preprocessUdf = udf((item_first_cate_cd: String, com_attr_group: String, com_attr_value_name: String) => {
      val rule = common_rules((item_first_cate_cd, com_attr_group)).getRule
      rule match {
        case "matching" => matching_rules((item_first_cate_cd, com_attr_group)).preprocess(com_attr_value_name)
        case "range" => range_rules((item_first_cate_cd, com_attr_group)).preprocess(com_attr_value_name)
        case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${item_first_cate_cd} and com_attr_group ${com_attr_group}")
      }
    })
    val attr_set = valid_source.map((row) => {
                                 val key = common_rules((row.item_first_cate_cd, row.com_attr_group)).getCateKey(
                                                        row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd)
                                 row.copy(item_first_cate_cd = key._1, item_second_cate_cd = key._2, item_third_cate_cd = key._3)
                               })
                               .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"item_third_cate_cd",
                                        $"com_attr_group", $"com_attr_value_cd", $"com_attr_value_name")
                               .agg(countDistinct($"item_sku_id").as("com_attr_value_count"))
                               .withColumn("com_attr_value_preprocess", preprocessUdf($"item_first_cate_cd", $"com_attr_group", $"com_attr_value_name"))
                               .collect()
                               .groupBy((row) => (row.getAs[String]("item_first_cate_cd"), row.getAs[String]("item_second_cate_cd"),
                                                  row.getAs[String]("item_third_cate_cd"), row.getAs[String]("com_attr_group")))
                               .map {
                                 case (k, v) => {
                                   val rule = common_rules((k._1, k._4))
                                   val sorted_v = v.map((e) => (e.getAs[String]("com_attr_value_cd"), e.getAs[String]("com_attr_value_name"),
                                                                e.getAs[Long]("com_attr_value_count"), e.getAs[Seq[String]]("com_attr_value_preprocess").toArray))
                                                   .sortWith((value_a, value_b) => {
                                                     rule.sort((value_a._1, value_a._2, value_a._3), (value_b._1, value_b._2, value_b._3))
                                                   })
                                   (k, sorted_v)
                                 }
                               }
    attr_set
  }
  private def getSource(): Dataset[Source] = {
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
        |select item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,item_sku_id,sku_name,barndname_full,colour,size
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
    val source_sku_meta = source_sku_da.join(source_sku_da_mkt, Seq("item_sku_id"), "left")
                                       .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"item_third_cate_cd",
                                                $"item_sku_id", $"sku_name", $"barndname_full", $"colour", $"size")
                                       .agg(max($"jd_prc").alias("jd_prc"))
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
    val source = source_sku_meta.join(source_attr, Seq("item_sku_id"))
                                .filter(filterCondition($"item_first_cate_cd", $"com_attr_cd"))
                                .withColumn("com_attr_group", toAttrGroup($"item_first_cate_cd", $"com_attr_cd"))
                                .as[Source]
                                .cache()
    source
  }
  private def getAttrCdNameMap(source: Dataset[Source]): Map[String, Map[String, String]] = {
    import spark.implicits._
    source.select($"item_first_cate_cd", $"com_attr_cd", $"com_attr_name")
          .distinct()
          .collect()
          .groupBy((row) => row.getAs[String]("item_first_cate_cd"))
          .map {case (k, v) => (k, v.map((e) => (e.getAs[String]("com_attr_cd"), e.getAs[String]("com_attr_name"))).toMap)}
          .toMap
  }
  private def getValidSource(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => common_rules((row.item_first_cate_cd, row.com_attr_group)).filterValid(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getRevision(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => common_rules((row.item_first_cate_cd, row.com_attr_group)).filterRevision(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getAlternative(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => !common_rules((row.item_first_cate_cd, row.com_attr_group)).filterRevision(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getRequiredAttr(source: Dataset[Source]): Map[(String, String), Set[(String, String)]] = {
    import spark.implicits._
    val cate_count = source.select($"item_first_cate_cd", $"item_second_cate_cd", $"item_sku_id")
                           .groupBy($"item_first_cate_cd", $"item_second_cate_cd")
                           .agg(countDistinct($"item_sku_id").as("cate_count"))
    val attr_count = source.select($"item_first_cate_cd", $"item_second_cate_cd",
                                   $"item_sku_id", $"com_attr_cd", $"com_attr_name")
                           .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"com_attr_cd", $"com_attr_name")
                           .agg(countDistinct($"item_sku_id").as("attr_count"))
    val required_attr = attr_count.join(cate_count, Seq("item_first_cate_cd", "item_second_cate_cd"))
                                  .withColumn("fraction", $"attr_count".cast("double") / $"cate_count".cast("double"))
                                  .filter($"fraction" >= 0.5)
                                  .collect()
                                  .groupBy((row) => (row.getAs[String]("item_first_cate_cd"), row.getAs[String]("item_second_cate_cd")))
                                  .map {case (k, v) => (k, v.map((e) => (e.getAs[String]("com_attr_cd"), e.getAs[String]("com_attr_name"))).toSet)}
                                  .toMap
    required_attr
  }
  private def getSupplement(source: Dataset[Source], required_attr: Map[(String, String), Set[(String, String)]]): Dataset[Source] = {
    import spark.implicits._
    val supplement = source.groupByKey((row) => row.copy(com_attr_cd = null, com_attr_name = null, com_attr_value_cd = null,
                                                         com_attr_value_name = null, com_attr_group = null))
                           .flatMapGroups((k, v) => {
                             val exist_attr = v.map((e) => (e.com_attr_cd, e.com_attr_name)).toSet
                             val supplement_attr = required_attr((k.item_first_cate_cd, k.item_second_cate_cd)) -- exist_attr
                             supplement_attr.map((e) => {
                               k.copy(com_attr_cd = e._1, com_attr_name = e._2, com_attr_group = attr_group_map(k.item_first_cate_cd)(e._1))
                             })
                           })
                           .cache()
    supplement
  }
}