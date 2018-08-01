package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MakeUserDict {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MakeUserDict")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val log1 = new log.ColorLog()
    val log2 = new log.BrandLog()
    val log3 = new log.DeduplicatedConceptLog()
    val log4 = new log.ProductWordLog()

    val df = log1.getLogDF(spark.sparkContext, args).unionAll(
      log2.getLogDF(spark.sparkContext, args)).unionAll(
        log3.getLogDF(spark.sparkContext, args)).unionAll(
          log4.getLogDF(spark.sparkContext, args))
      .select($"color".alias("word"))

    tools.LoadDataToHive.loadParquetDataToHive(
      spark.sparkContext,
      df.distinct,
      "taxonomy_internal_user_dict",
      args) 
  }
}
