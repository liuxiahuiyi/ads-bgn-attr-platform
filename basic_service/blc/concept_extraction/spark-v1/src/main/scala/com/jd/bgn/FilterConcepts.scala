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
import com.jd.bgn.tools.LoadDataToHive.loadParquetDataToHive

object FilterConcepts {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("FilterConcepts")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val phraseRDD = (new log.SkuPhraseFromNatural()).getLogDF(spark.sparkContext, args)
        .select("concept", "frequency")
        .filter($"frequency" > 100)
        .rdd
    val rdd = checkWordType(spark.sparkContext, args, phraseRDD)
    val schema = StructType(
      Array(StructField("concept", StringType, true)))
    val hiveContext = new HiveContext(spark.sparkContext)
    val filterConceptDF = hiveContext.createDataFrame(rdd, schema).distinct
    loadParquetDataToHive(spark.sparkContext,
      filterConceptDF,
      "taxonomy_internal_sku_concept_from_natural",
      args)
  }

  def checkWordType(sc: SparkContext, args: CommonArgs, data: RDD[Row]): RDD[Row] = {
    val color = "金银蓝黑黄橙粉红绿灰白紫青棕色棕".toSet
    val stopwordLog = (new log.StopwordLog()).getLogDF(sc, args)
    val productwordlog = (new log.ProductWordLog()).getLogDF(sc, args)
    val brandLog = (new log.BrandLog()).getLogDF(sc, args)
    val stopWords = stopwordLog.select("stopword").rdd.map(_(0).toString).collect.toSet
    val products = productwordlog.select("product_word").rdd.map(_(0).toString).collect.toSet
    val brands = brandLog.select("brand").rdd.map(_(0).toString).collect.toSet

    data.filter { e =>
      val x = e(0).toString
      var r = false
      if (x.length <= 1) r = true
      if (!x.matches("[\\u4e00-\\u9fff]+")) r = true
      if (color.contains(x(x.length -1))) r = true
      if (!r) {
        stopWords.foreach { s =>
          if (x.contains(s)) r = true
        }
      }
      if (!r) {
        products.foreach { p =>
          if (x.contains(p)) r = true
        }
      }
      if (!r) {
        val match_brand = brands.par.filter(t =>
            x.contains(t) || t.contains(x))
        if (match_brand.size > 0) r = true
      }
      !r
    }
  } 
}
