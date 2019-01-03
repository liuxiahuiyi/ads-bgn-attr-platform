package com.jd.bgn.item_img2txt

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SaveMode, Dataset}
import org.apache.spark.sql.functions._
import scala.util.matching.Regex._
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles

class Transfomer(
  config: Config
) extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private final val spark = SparkEnv.getSession
  def run(): Unit = {
  	val sc = spark.sparkContext
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sc.addFile(config.library_hdfs, true)
    sc.addFile(config.ocr_trained_data_hdfs, true)
    val (target_retain, under_transform) = getUnderTransform()
    logger.info(s"count of target_retain is: ${target_retain.count}")
    logger.info(s"count of under_transform is: ${under_transform.count}")

    val target_new = transform(under_transform)
    logger.info(s"count of target_new is: ${target_new.count}")

    writeTable(target_retain, target_new)
    spark.close()
  }
  private def writeTable(target_retain: Dataset[Target], target_new: Dataset[Target]): Unit = {
    import spark.implicits._
    if (config.item_first_cate_cds == "all") {
      spark.sql(s"ALTER TABLE ${config.target_db_table} DROP IF EXISTS PARTITION (dp='ACTIVE') PURGE")
    } else {
      for (first_cate <- config.item_first_cate_cds.split(",").filter(_ != "")) {
        spark.sql(s"ALTER TABLE ${config.target_db_table} DROP IF EXISTS PARTITION (dp='ACTIVE', end_date='9999-12-31', item_first_cate_cd='${first_cate}') PURGE")
      }
    }
    
    target_retain.select($"item_id", $"item_img_url", $"item_img_txt", $"start_date",
                         lit("ACTIVE").alias("dp"), lit("9999-12-31").alias("end_date"), $"item_first_cate_cd")
                 .union(target_new.select($"item_id", $"item_img_url", $"item_img_txt", $"start_date",
                                          lit("ACTIVE").alias("dp"), lit("9999-12-31").alias("end_date"), $"item_first_cate_cd"))
                 .repartition(10)
                 .write
                 .mode(SaveMode.Append)
                 .insertInto(config.target_db_table)
  }
  private def transform(under_transform: Dataset[Target]): Dataset[Target] = {
    import spark.implicits._
    val count = under_transform.count
    under_transform.sample(false, Math.min(2e6 / count, 1))
                   .map((row) => {
                   	 val text = new Ocr().run(row.item_img_url)
                     row.copy(item_img_txt = text)
                   })
                   .cache()
  }
  private def getUnderTransform(): (Dataset[Target], Dataset[Target]) = {
    import spark.implicits._
    val skus = spark.sql(
      s"""
        |select item_first_cate_cd,item_id
        |  from gdm.gdm_m03_item_sku_act
        |  where dt='${config.calDate(-1)}' ${config.getFirstCateCondition} and
        |  sku_valid_flag=1 and sku_status_cd!='3000' and sku_status_cd!='3010' and item_id is not null
      """.stripMargin).distinct()
    val item_big_info = spark.sql(
      s"""
        |select product_id as item_id, wdis
        |  from fdm.fdm_exaitem_biginfo_biginfo_product_1_chain
        |  where dp='ACTIVE' and start_date='${config.calDate(-1)}'
      """.stripMargin).groupBy($"item_id")
                      .agg(max($"wdis").alias("item_img_url"))
    val items = skus.join(item_big_info, Seq("item_id"))
                    .withColumn("item_img_txt", lit(null))
                    .withColumn("start_date", lit(config.date))
                    .as[Target]
                    .flatMap((row) => {
                      val urls = new WdisParser().parse(row.item_img_url)
                      var rows = Array[Target]()
                      for (url <- urls) {
                        rows = rows :+ row.copy(item_img_url = url)
                      }
                      rows
                    })
    val target = spark.sql(
      s"""
        |select item_first_cate_cd, item_id, item_img_url, item_img_txt, start_date
        |  from ${config.target_db_table}
        |  where dp='ACTIVE' and start_date>='${config.calDate(-180)}' ${config.getFirstCateCondition}
      """.stripMargin).as[Target]
    val tmp = target.join(items.select($"item_first_cate_cd".alias("item_first_cate_cd_r"),
                                       $"item_id".alias("item_id_r"),
                                       $"item_img_url".alias("item_img_url_r"),
                                       $"item_img_txt".alias("item_img_txt_r"),
                                       $"start_date".alias("start_date_r")),
                          $"item_first_cate_cd" === $"item_first_cate_cd_r" &&
                          $"item_id" === $"item_id_r" &&
                          $"item_img_url" === $"item_img_url_r", "full")
                    .cache()
    val under_transform = tmp.filter($"item_first_cate_cd".isNull && $"item_id".isNull && $"item_img_url".isNull)
                             .select($"item_first_cate_cd_r".alias("item_first_cate_cd"),
                                     $"item_id_r".alias("item_id"),
                                     $"item_img_url_r".alias("item_img_url"),
                                     $"item_img_txt_r".alias("item_img_txt"),
                                     $"start_date_r".alias("start_date"))
                             .as[Target]
                             .cache()
    val tmp1 = tmp.filter($"item_first_cate_cd".isNotNull && $"item_id".isNotNull && $"item_img_url".isNotNull &&
                          $"item_first_cate_cd_r".isNotNull && $"item_id_r".isNotNull && $"item_img_url_r".isNotNull)
                  .select($"item_first_cate_cd", $"item_id", $"item_img_url", $"item_img_txt", $"start_date")
                  .as[Target]
    val tmp2 = tmp.filter($"item_first_cate_cd_r".isNull && $"item_id_r".isNull && $"item_img_url_r".isNull)
                  .select($"item_first_cate_cd", $"item_id", $"item_img_url", $"item_img_txt", $"start_date")
                  .as[Target]
    val tmp3 = tmp2.join(tmp.select($"item_first_cate_cd_r", $"item_id_r")
                            .filter($"item_first_cate_cd_r".isNotNull && $"item_id_r".isNotNull)
                            .distinct(),
                         $"item_first_cate_cd" === $"item_first_cate_cd_r" &&
                         $"item_id" === $"item_id_r", "left")
    val tmp4 = tmp3.filter($"item_first_cate_cd_r".isNull && $"item_id_r".isNull)
                   .select($"item_first_cate_cd", $"item_id", $"item_img_url", $"item_img_txt", $"start_date")
                   .as[Target]
    val target_retain = tmp1.union(tmp4).cache()
    return (target_retain, under_transform)
  }
}
