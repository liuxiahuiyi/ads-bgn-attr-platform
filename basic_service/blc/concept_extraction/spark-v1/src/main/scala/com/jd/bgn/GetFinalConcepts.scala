package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

object GetFinalConcepts {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GetFinalConcepts")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val log_object = new log.SkuSegmentationLog()
    val logRDD = log_object.getLogDF(spark.sparkContext, args)
                  .select("sku_segment")

    val conceptRDD = logRDD.flatMap { x =>
      val sku_name = x(0).toString
      val phrases = sku_name.trim().split(" +")
      phrases
    }.groupBy($"value".alias("concept"))
     .agg(count($"value").alias("frequency"))
     .filter($"frequency" > 100)
     .rdd
    
    val rdd = FilterConcepts.checkWordType(spark.sparkContext, args, conceptRDD)
    val schema = StructType(
      Array(StructField("concept", StringType, true)))
    val hiveContext = new HiveContext(spark.sparkContext)
    val df = hiveContext.createDataFrame(rdd, schema).distinct
    tools.LoadDataToHive.loadParquetDataToHive(
      spark.sparkContext, 
      df,
      "taxonomy_internal_sku_concept_from_segmentation",
      args)
  }
}
