package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.parse
import com.jd.bgn.tools.LoadDataToHive.loadLineDataToHive

object MakeUserDict {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MakeUserDict")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val args = new CommonArgs(arguments)
    var cal = Calendar.getInstance
    cal.setTime(args.date)
    cal.add(Calendar.DATE, -1)
    val dateIdx = arguments.indexOf("--date") + 1
    val strdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val arguments2 = arguments.updated(dateIdx, strdate)
    val args2 = new CommonArgs(arguments2)

    val log1 = new log.SkuAttributeLog()
    val skuLog = log1.getLogDF(sc, args)
    val productlog = (new log.ProductWordLog()).getLogDF(sc, args2)
    val brandLog = (new log.BrandLog()).getLogDF(sc, args2)
    val products = productlog.select("product_word").rdd.map(_(0).toString)
    val brands = brandLog.select("brand").rdd.map(_(0).toString)

    val skuBrand = skuLog.select("brandname_en", "brandname_cn", "brandname_full")
      .flatMap { x =>
        Array(x(0).toString, x(1).toString, x(2).toString)
      }.rdd.union(brands).distinct.filter(_.size > 1)
    loadLineDataToHive(sc, skuBrand.repartition(10),
      "taxonomy_internal_sku_brands_v2", args)

    val skuProduct = skuLog.select("product_words_list")
      .flatMap { x =>
        val getStringOrElse = (index: Int, value: String) => {
          (if (!x.isNullAt(index)) x.getString(index) else value)
        }   
        getStringOrElse(0,"").toString.trim().replace(", ", ",").split(",")
      }.rdd.union(products).distinct.filter(_.size > 1)
    loadLineDataToHive(sc, skuProduct.repartition(10),
      "taxonomy_internal_product_words_v2", args)

    val brandRDD = skuBrand.filter(_.matches("[\\u4e00-\\u9fff]+"))
    val productRDD = skuProduct.filter(_.matches(".*[\\u4e00-\\u9fff].*"))

    val log2 = new log.LabelProductLog()
    val log3 = new log.LabelSegmentationLog()
    val labelRDD = log2.getLogDF(sc, args).select("label")
      .unionAll(log3.getLogDF(sc, args).select("label"))
      .rdd.flatMap { x =>
        parse(x(0).toString.replace("\\\\\"", ""), false)
          .values.asInstanceOf[List[Map[String, Any]]]
          .map { e => e.get("quote").get.toString.trim()}
      }.distinct
      .filter(x => x.matches(".*[\\u4e00-\\u9fff].*") && x.length > 1 && x.length <= 6)

    val dictRDD = DeduplicateConcepts.checkConceptDuplicate(
      sc.union(Array(brandRDD, labelRDD, productRDD)).map(_.toLowerCase).distinct)

    loadLineDataToHive(sc, dictRDD.repartition(10),
      "taxonomy_internal_user_dict_v2", args)
  }
}
