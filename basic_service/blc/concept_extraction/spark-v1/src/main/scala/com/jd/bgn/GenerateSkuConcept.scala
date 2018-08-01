package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

object GenerateSkuConcept {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GenerateSkuConcept")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val log1 = new log.BasicConceptLog()
    val concepts = log1.getLogDF(spark.sparkContext, args)
      .select("concept").rdd.map(_(0).toString).collect.toSet
    val log2 = new log.ProductWordLog()
    val products = log2.getLogDF(spark.sparkContext, args)
      .select("product_word").rdd.map(_(0).toString).collect.toSet

    val log3 = new log.SkuAttributeLog()
    val attrDF = log3.getLogDF(spark.sparkContext, args)
      .select("item_sku_id", "sku_name")
    val log4 = new log.SkuSegmentationLog()
    val segDF = log4.getLogDF(spark.sparkContext, args)
      .select("sku_id", "sku_segment")
    val data = segDF.join(attrDF, segDF("sku_id")===attrDF("item_sku_id"), "left_outer")
      .rdd.flatMap { e =>
      val sku_id = e(0).toString()
      val process_sku_name = e(1).toString()
      val sku_name = e(3).toString()
      val words = process_sku_name.split(" +")
      var curent_pruducts: Set[String] = Set()
      var curent_concepts: Set[String] = Set()
      var pruducts_result: List[String] = List()
      var concepts_result: List[String] = List()
      words.foreach { w =>
        if (products.contains(w)) {
          if (!curent_pruducts.contains(w)) {
            var idx_arr: Array[Int] = Array()
            var idx = sku_name.indexOf(w)
            while (idx > -1) {
              idx_arr :+= idx
              idx = sku_name.indexOf(w, idx + 1)
            }
            pruducts_result :+= ("pro_" + w + ":" + idx_arr.mkString(","))
            curent_pruducts += w
          }
        } else if (concepts.contains(w)) {
          if (!curent_concepts.contains(w)) {
            var idx_arr: Array[Int] = Array()
            var idx = sku_name.indexOf(w)
            while (idx > -1) {
              idx_arr :+= idx
              idx = sku_name.indexOf(w, idx + 1)
            }
            concepts_result :+= (w + ":" + idx_arr.mkString(","))
            curent_concepts += w
          }
        }
      }
      if (concepts_result.isEmpty || pruducts_result.isEmpty) {
        None
      } else {
        val concepts_str = concepts_result.mkString(";")
        val pruducts_str = pruducts_result.mkString(";")
        Some(s"""${sku_id}\01${sku_name.replace("\01", "")}\01${concepts_str};${pruducts_str}""")
      }
    }
    /*val hiveContext = new HiveContext(spark.sparkContext)
    val schema = StructType(
      Array(StructField("item_sku_id", StringType, true),
        StructField("sku_name", StringType, true),
        StructField("concept_product_index", StringType, true)))
    val df = hiveContext.createDataFrame(data, schema)
    */
    tools.LoadDataToHive.loadLineDataToHive(
      spark.sparkContext, 
      data,
      "taxonomy_sku_id_name_concept_product",
      args)
  }
}
