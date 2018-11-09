package com.jd.bgn.rule_engine

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SaveMode, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import scala.util.matching.Regex._
import collection.mutable.WrappedArray

import com.jd.bgn.rules.{Common, Matching, Range, Age}
import com.jd.bgn.defenitions.{Config, Source, Target}
import com.jd.bgn.utils.{AttrSetFileIO, AttrValueUtil}

class RuleEngine(
  config: Config,
  attr_group_map: Map[String, Map[String, String]],
  common_rules: Map[(String, String), Common],
  matching_rules: Map[(String, String), Matching],
  range_rules: Map[(String, String), Range],
  age_rules: Map[(String, String), Age]
) extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private final val spark = SparkEnv.getSession

  private final var config_broadcast: Broadcast[Config] = null
  private final var attr_group_map_broadcast: Broadcast[Map[String, Map[String, String]]] = null
  private final var common_rules_broadcast: Broadcast[Map[(String, String), Common]] = null
  private final var matching_rules_broadcast: Broadcast[Map[(String, String), Matching]] = null
  private final var range_rules_broadcast: Broadcast[Map[(String, String), Range]] = null
  private final var age_rules_broadcast: Broadcast[Map[(String, String), Age]] = null
  private final var overall_attr_value_broadcast: Broadcast[Map[String, Array[(String, String, String)]]] = null
  private final var attr_set_broadcast: Broadcast[Map[(String, String, String, String), Array[(String, String, Long, Array[String])]]] = null

  def run(): Unit = {
    val sc = spark.sparkContext
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    config_broadcast = sc.broadcast(config)
    attr_group_map_broadcast = sc.broadcast(attr_group_map)
    // debug
    attr_group_map.foreach((e) => {
      println(e._1)
      e._2.foreach(println("attr_group_map:", _))
    })
    // debug
    matching_rules_broadcast = sc.broadcast(matching_rules)
    range_rules_broadcast = sc.broadcast(range_rules)
    age_rules_broadcast = sc.broadcast(age_rules)

    val (source, overall_attr_value) = getSource()
    logger.info(s"count of source data is: ${source.count}")
    logger.info(s"schema of source data is: ${source.schema}")
    overall_attr_value_broadcast = sc.broadcast(overall_attr_value)
    // debug start
    overall_attr_value.foreach{ case (k, v) => v.foreach((e) => println(s"overall_attr_value: ${k}->${e._1}, ${e._2}, ${e._3}")) }
    // debug end

    addAttrValueScarce(source)
    common_rules_broadcast = sc.broadcast(common_rules)

    val required_attr = getRequiredAttr(source)
    // debug start
    required_attr.foreach((e) => {
      println("item_first_cate_cd and item_second_cate_cd:", e._1)
      e._2.foreach(println("attr cd and name:", _))
    })
    // debug end
    val supplement = getSupplement(source, required_attr)
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
      attr_set = new AttrSetFileIO().read(s"./.ads_bgn_attr_platform_rule_engine_${attr_group_map.keys.mkString(",")}_attr_set")
    } else {
      attr_set = getAttrSet(valid_source)
      new AttrSetFileIO().write(attr_set, s"./.ads_bgn_attr_platform_rule_engine_${attr_group_map.keys.mkString(",")}_attr_set")
    }
    logger.info(s"size of attr set is: ${attr_set.size}")
    attr_set_broadcast = sc.broadcast(attr_set)
    // debug start
    attr_set.foreach((e) => {
      println("cate_and_attr_group:", e._1)
      e._2.foreach((se) => {
        println("attr_values_sorted:", se._1, se._2, se._3)
      })
    })
    // debug end

    val revision_under_rules = assembleUnderRules(revision, alternative)
    logger.info(s"count of revision_under_rules is: ${revision_under_rules.count}")
    logger.info(s"schema of revision_under_rules is: ${revision_under_rules.schema}")
    val supplement_under_rules = assembleUnderRules(supplement, alternative)
    logger.info(s"count of supplement_under_rules is: ${supplement_under_rules.count}")
    logger.info(s"schema of supplement_under_rules is: ${supplement_under_rules.schema}")

    val revision_target = execRules(revision_under_rules)
    logger.info(s"count of revision_target is: ${revision_target.count}")
    logger.info(s"schema of revision_target is: ${revision_target.schema}")
    val supplement_target = execRules(supplement_under_rules)
    logger.info(s"count of supplement_target is: ${supplement_target.count}")
    logger.info(s"schema of supplement_target is: ${supplement_target.schema}")

    writeTable(revision_target, "revision")
    writeTable(supplement_target, "supplement")
    spark.close()
  }
  private def writeTable(target: Dataset[Target], flag: String): Unit = {
    import spark.implicits._
    val item_first_cate_cds = attr_group_map.keys
    for (item_first_cate_cd <- item_first_cate_cds) {
      spark.sql(s"ALTER TABLE ${config.target_db_table} DROP IF EXISTS PARTITION (dt='${config.date}', flag='${flag}', item_first_cate_cd='${item_first_cate_cd}')")
    }
    target.select(
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
            col("com_attr_value_rem"),
            col("com_attr_group"),
            col("alt_attr_value_cd"),
            col("alt_attr_value_name"),
            col("old_attr_value_cd"),
            col("old_attr_value_name"),
            col("source_id") alias "source",
            lit(config_broadcast.value.date) alias "dt",
            lit(flag) alias "flag",
            col("item_first_cate_cd"))
          .repartition(config.repartition)
          .write
          .mode(SaveMode.Append)
          .insertInto(config_broadcast.value.target_db_table)
  }
  private def execRules(under_rules: Dataset[Target]): Dataset[Target] = {
    import spark.implicits._
    under_rules.map((row) => {
                 val revision = if (row.alt_attr_value_cd != null && row.alt_attr_value_cd != ""
                                    && row.alt_attr_value_name != null && row.alt_attr_value_name != "") {
                   (row.alt_attr_value_cd, row.alt_attr_value_name, 5)
                 } else {
                   val attr_rem_revision = common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group))
                                                                 .applyAttrValueRem(row.com_attr_value_rem, if (overall_attr_value_broadcast.value.contains(row.item_first_cate_cd)) overall_attr_value_broadcast.value(row.item_first_cate_cd) else Array[(String, String, String)]())
                   if (attr_rem_revision._1 != null && attr_rem_revision._2 != null) {
                     (attr_rem_revision._1, attr_rem_revision._2, 4)
                   } else {
                     val rule = common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).getRule
                     val columns = common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).getColumns
                     val metas = columns.map(row.getMeta(_))
                     val key = common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).getCateKey(
                       row.item_first_cate_cd, row.item_second_cate_cd, row.item_third_cate_cd)
                     if (!attr_set_broadcast.value.contains(key._1, key._2, key._3, row.com_attr_group)) {
                       (null, null, 0)
                     } else {
                       var attr_set_for_cate_attr = attr_set_broadcast.value((key._1, key._2, key._3, row.com_attr_group))
                       val current_index = attr_set_for_cate_attr.indexWhere((e) => e._1 == row.com_attr_value_cd && e._2 == row.com_attr_value_name)
                       if (current_index != -1) {
                         attr_set_for_cate_attr = attr_set_for_cate_attr.slice(current_index, current_index + 1) ++
                                                  attr_set_for_cate_attr.slice(0, current_index) ++
                                                  attr_set_for_cate_attr.slice(current_index + 1, attr_set_for_cate_attr.length)
                       }
                       val rule_revision = rule match {
                         case "matching" => matching_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).process(metas, attr_set_for_cate_attr)
                         case "range" => range_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).process(metas, attr_set_for_cate_attr)
                         case "age" => age_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).process(metas, attr_set_for_cate_attr)
                         case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${row.item_first_cate_cd} and com_attr_group ${row.com_attr_group}")
                       }
                       if (rule_revision._1 != null && rule_revision._2 != null) {
                         (rule_revision._1, rule_revision._2, 3)
                       } else if (current_index != -1) {
                         (row.com_attr_value_cd, row.com_attr_value_name, 0)
                       } else {
                         (null, null, 0)
                       }
                     }
                   }
                 }
                 row.copy(old_attr_value_cd = row.com_attr_value_cd, old_attr_value_name = row.com_attr_value_name,
                          com_attr_value_cd = revision._1, com_attr_value_name = revision._2, source_id = (row.source_id.toInt + revision._3).toString)
               })
               .filter((row) => row.com_attr_value_cd != null && row.com_attr_value_name != null)
               .cache()
  }
  private def assembleUnderRules(source: Dataset[Source], alternative: Dataset[Source]): Dataset[Target] = {
    import spark.implicits._
    val alt = alternative.select($"item_sku_id".as("alt_item_sku_id"), $"com_attr_group".as("alt_attr_group"),
                                 $"com_attr_cd".as("alt_attr_cd"), $"com_attr_name".as("alt_attr_name"),
                                 $"com_attr_value_cd".as("alt_attr_value_cd"), $"com_attr_value_name".as("alt_attr_value_name"))
    val under_rules = source.join(alt, $"item_sku_id" === $"alt_item_sku_id" && $"com_attr_group" === $"alt_attr_group" &&
                                  $"com_attr_cd" =!= $"alt_attr_cd" && $"com_attr_name" =!= $"alt_attr_name", "left")
                            .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"item_third_cate_cd",
                                     $"item_sku_id", $"sku_name", $"barndname_full", $"colour", $"size", $"jd_prc", $"item_type", $"item_img_txt",
                                     $"com_attr_cd", $"com_attr_name", $"com_attr_value_cd", $"com_attr_value_name",
                                     $"com_attr_value_rem", $"com_attr_group")
                            .agg(first($"alt_attr_value_cd").as("alt_attr_value_cd"), first($"alt_attr_value_name").as("alt_attr_value_name"))
                            .withColumn("old_attr_value_cd", lit(null))
                            .withColumn("old_attr_value_name", lit(null))
                            .withColumn("source_id", lit(config_broadcast.value.source_id))
                            .as[Target]
                            .cache()
    under_rules
  }
  private def getAttrSet(valid_source: Dataset[Source]): Map[(String, String, String, String), Array[(String, String, Long, Array[String])]] = {
    import spark.implicits._
    val preprocessUdf = udf((item_first_cate_cd: String, com_attr_group: String, com_attr_value_name: String) => {
      val rule = common_rules_broadcast.value((item_first_cate_cd, com_attr_group)).getRule
      rule match {
        case "matching" => matching_rules_broadcast.value((item_first_cate_cd, com_attr_group)).preprocess(com_attr_value_name)
        case "range" => range_rules_broadcast.value((item_first_cate_cd, com_attr_group)).preprocess(com_attr_value_name)
        case "age" => age_rules_broadcast.value((item_first_cate_cd, com_attr_group)).preprocess(com_attr_value_name)
        case _ => throw new Error(s"unknown rule ${rule} in item_first_cate_cd ${item_first_cate_cd} and com_attr_group ${com_attr_group}")
      }
    })
    val attr_set = valid_source.map((row) => {
                                 val key = common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).getCateKey(
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
  private def getSource(): (Dataset[Source], Map[String, Array[(String, String, String)]]) = {
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
        |select item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,item_id,item_sku_id,sku_name,barndname_full,colour,size,item_type
        |  from gdm.gdm_m03_item_sku_act
        |  where item_first_cate_cd in ${item_first_cate_cds} and dt='${config.date}' and
        |  sku_valid_flag=1 and sku_status_cd!='3000' and sku_status_cd!='3010' and item_sku_id is not null and sku_name is not null
      """.stripMargin).cache()
    val third_first_cate_mapping = source_sku_da.select($"item_first_cate_cd", $"item_third_cate_cd")
                                                .distinct()
                                                .collect()
                                                .map((row) => (row.getAs[String]("item_third_cate_cd"), row.getAs[String]("item_first_cate_cd")))
                                                .toMap
    val item_third_cate_cds = third_first_cate_mapping.keys
                                                      .map("'" + _ + "'")
                                                      .mkString("(", "," ,")")
    val remark_enable_first_cate_ids = common_rules.filter{ case (k, v) => v.getRemarkEnable }
                                                   .map { case (k, v) => k._1 }
                                                   .toSet

    val source_sku_da_mkt = spark.sql(
      s"""
        |select item_sku_id,jd_prc
        |  from gdm.gdm_m03_mkt_item_sku_da
        |  where dt='${config.date}' and item_first_cate_cd in ${item_first_cate_cds}
      """.stripMargin).groupBy($"item_sku_id")
                      .agg(max($"jd_prc").alias("jd_prc"))
    val listToStringUdf = udf((list: WrappedArray[String]) => {
      list.mkString("\n")
    })
    val source_item_detail = spark.sql(
      s"""
        |select item_id,item_img_txt
        |  from bgn.item_img2txt
        |  where dp='ACTIVE' and item_first_cate_cd in ${item_first_cate_cds}
      """.stripMargin).groupBy($"item_id")
                      .agg(listToStringUdf(collect_list($"item_img_txt")).alias("item_img_txt"))

    val source_sku_meta = source_sku_da.join(source_sku_da_mkt, Seq("item_sku_id"), "left")
                                       .join(source_item_detail, Seq("item_id"), "left")
                                       .drop($"item_id")

    val source_attr = spark.sql(
      s"""
        |select cate_id as item_third_cate_cd,item_sku_id,com_attr_cd,com_attr_name,
        |  com_attr_value_cd,com_attr_value_name,com_attr_value_rem
        |  from gdm.gdm_m03_item_sku_ext_attr_da
        |  where dt='${config.date}' and cate_id in ${item_third_cate_cds}
        |  union distinct
        |  select cate_id as item_third_cate_cd,item_sku_id,com_attr_cd,com_attr_name,
        |  com_attr_value_cd,com_attr_value_name,com_attr_value_rem
        |  from gdm.gdm_m03_item_sku_spec_par_da
        |  where dt='${config.date}' and cate_id in ${item_third_cate_cds}
      """.stripMargin).cache()
    val attrValueLength = udf((com_attr_value_name: String) => AttrValueUtil.length(com_attr_value_name, "word"))
    val attrValueFilter = udf((item_first_cate_cd: String, com_attr_value_name: String) => remark_enable_first_cate_ids.contains(item_first_cate_cd) &&
                                                                                           com_attr_value_name != null &&
                                                                                           com_attr_value_name.trim.length > 1 &&
                                                                                           !com_attr_value_name.contains("其他") &&
                                                                                           !com_attr_value_name.contains("其它"))
    val toFirstCateCd = udf((item_third_cate_cd: String) => third_first_cate_mapping(item_third_cate_cd))
    val overall_attr_value = source_attr.withColumn("item_first_cate_cd", toFirstCateCd($"item_third_cate_cd"))
                                        .filter(attrValueFilter($"item_first_cate_cd", $"com_attr_value_name"))
                                        .groupBy($"item_first_cate_cd", $"com_attr_value_cd", $"com_attr_value_name")
                                        .agg(countDistinct($"item_sku_id").alias("count"))
                                        .sort($"item_first_cate_cd", attrValueLength($"com_attr_value_name").desc, length($"com_attr_value_name").desc, $"count".desc)
                                        .select($"item_first_cate_cd", $"com_attr_value_cd", $"com_attr_value_name")
                                        .collect()
                                        .map((row) => {
                                          val item_first_cate_cd = row.getAs[String]("item_first_cate_cd")
                                          val com_attr_value_cd = row.getAs[String]("com_attr_value_cd")
                                          val com_attr_value_name = row.getAs[String]("com_attr_value_name")
                                          (item_first_cate_cd, com_attr_value_cd, com_attr_value_name,
                                           AttrValueUtil.getPattern(com_attr_value_name.replaceAll("\\pP|\\pS", ""), ".{0,1}"))
                                        })
                                        .groupBy(_._1)
                                        .map {
                                          case (k, v) => (k, v.slice(0, (v.length * 0.8).toInt).map((e) => (e._2, e._3, e._4)))
                                        }
                                        .toMap
    val toAttrGroup = udf((item_first_cate_cd: String, com_attr_cd: String) =>
      attr_group_map_broadcast.value(item_first_cate_cd)(com_attr_cd))
    val filterCondition = udf((item_first_cate_cd: String, com_attr_cd: String) =>
      attr_group_map_broadcast.value.contains(item_first_cate_cd) &&
      attr_group_map_broadcast.value(item_first_cate_cd).contains(com_attr_cd))
    val source = source_sku_meta.join(source_attr.filter(expr(s"com_attr_cd in ${com_attr_cds}")),
                                      Seq("item_third_cate_cd", "item_sku_id"))
                                .filter(filterCondition($"item_first_cate_cd", $"com_attr_cd"))
                                .withColumn("com_attr_group", toAttrGroup($"item_first_cate_cd", $"com_attr_cd"))
                                .as[Source]
                                .cache()
    (source, overall_attr_value)
  }
  private def getValidSource(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).filterValid(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getRevision(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).filterRevision(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getAlternative(source: Dataset[Source]): Dataset[Source] = {
    source.filter((row) => !common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).filterRevision(row.com_attr_value_cd, row.com_attr_value_name) &&
                           common_rules_broadcast.value((row.item_first_cate_cd, row.com_attr_group)).filterValid(row.com_attr_value_cd, row.com_attr_value_name))
          .cache()
  }
  private def getRequiredAttr(source: Dataset[Source]): Map[(String, String), Set[(String, String)]] = {
    import spark.implicits._
    val cate_count = source.select($"item_first_cate_cd", $"item_second_cate_cd", $"item_sku_id")
                           .groupBy($"item_first_cate_cd", $"item_second_cate_cd")
                           .agg(countDistinct($"item_sku_id").as("cate_count"))
    val attr_count = source.select($"item_first_cate_cd", $"item_second_cate_cd",
                                   $"item_sku_id", $"com_attr_cd", $"com_attr_name", $"com_attr_group")
                           .groupBy($"item_first_cate_cd", $"item_second_cate_cd", $"com_attr_cd", $"com_attr_name", $"com_attr_group")
                           .agg(countDistinct($"item_sku_id").as("attr_count"))
    val required_attr = attr_count.join(cate_count, Seq("item_first_cate_cd", "item_second_cate_cd"))
                                  .withColumn("fraction", $"attr_count".cast("double") / $"cate_count".cast("double"))
                                  .filter((row) => common_rules_broadcast.value(row.getAs[String]("item_first_cate_cd"), row.getAs[String]("com_attr_group"))
                                                                         .filterRequiredAttr(row.getAs[Double]("fraction")))
                                  .collect()
                                  .groupBy((row) => (row.getAs[String]("item_first_cate_cd"), row.getAs[String]("item_second_cate_cd")))
                                  .map {case (k, v) => (k, v.map((e) => (e.getAs[String]("com_attr_cd"), e.getAs[String]("com_attr_name"))).toSet)}
                                  .toMap
    required_attr
  }
  private def getSupplement(source: Dataset[Source], required_attr: Map[(String, String), Set[(String, String)]]): Dataset[Source] = {
    import spark.implicits._
    val supplement = source.groupByKey((row) => row.copy(com_attr_cd = null, com_attr_name = null, com_attr_value_cd = null,
                                                         com_attr_value_name = null, com_attr_value_rem = null, com_attr_group = null))
                           .flatMapGroups((k, v) => {
                             if (required_attr.contains((k.item_first_cate_cd, k.item_second_cate_cd))) {
                               val exist_attr = v.map((e) => (e.com_attr_cd, e.com_attr_name)).toSet
                               val supplement_attr = required_attr((k.item_first_cate_cd, k.item_second_cate_cd)) -- exist_attr
                               supplement_attr.map((e) => {
                                 k.copy(com_attr_cd = e._1, com_attr_name = e._2, com_attr_group = attr_group_map_broadcast.value(k.item_first_cate_cd)(e._1))
                               })
                             } else {
                               Array[Source]()
                             }
                           })
    getRevision(supplement).cache()
  }
  private def addAttrValueScarce(source: Dataset[Source]): Unit = {
    import spark.implicits._
    val attr_value_count = source.groupBy($"item_first_cate_cd", $"com_attr_group",
                                          $"com_attr_value_cd", $"com_attr_value_name")
                                 .agg(countDistinct($"item_sku_id").as("attr_value_count"))
    val attr_group_count = source.groupBy($"item_first_cate_cd", $"com_attr_group")
                                 .agg(countDistinct($"item_sku_id").as("attr_group_count"))
    val attr_value_proportion = attr_value_count.join(attr_group_count, Seq("item_first_cate_cd", "com_attr_group"))
                                                .withColumn("attr_value_proportion", $"attr_value_count".cast("double") / $"attr_group_count".cast("double"))
                                                .collect()
                                                .foreach((row) => {
                                                  common_rules(row.getAs[String]("item_first_cate_cd"), row.getAs[String]("com_attr_group")).addAttrValueScarce(
                                                    row.getAs[String]("com_attr_value_name"), row.getAs[Double]("attr_value_proportion"))
                                                })
    
  }

}