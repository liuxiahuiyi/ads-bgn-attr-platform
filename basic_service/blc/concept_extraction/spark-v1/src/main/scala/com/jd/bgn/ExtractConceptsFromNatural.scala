package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.jd.bgn.tools.LoadDataToHive.loadParquetDataToHive

object ExtractConceptsFromNatural {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtractConceptsFromNatural")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val log_ = new log.SkuAttributeLog()
    val skuLog = log_.getLogDF(spark.sparkContext, args) 

    var punct = """:!,:;]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
    |﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
    |々‖•·ˇˉ―--′’”£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻ 　  
    |︽︿﹁﹃﹙﹛﹝（｛“‘-—_…/""".stripMargin.replaceAll("\n", "")
    val reChar = ")?([{*+.".map("\\" + _).mkString("|")
    punct = punct.toArray.mkString("|") + "|" + reChar
    val phrase_len_limit = 6

    val skuPhrase = skuLog.select("sku_name")
      .flatMap { e =>
        val sku_name = e(0).toString.replaceAll(punct, " ")
        val phrases = sku_name.trim().split(" +")
        phrases.filter(x => x.length <= phrase_len_limit && x.length > 1)
      }.groupBy($"value".alias("concept"))
        .agg(count($"value").alias("frequency"))
    loadParquetDataToHive(spark.sparkContext, 
      skuPhrase,
      "taxonomy_internal_sku_phrase_from_natural",
      args)
    val color = "金银蓝黑黄橙粉红绿灰白紫青棕色棕".toSet
    val skuColor = skuPhrase.rdd.filter { x =>
      val concept = x(0).toString
      color.contains(concept(concept.length -1))
    }
    val hiveContext = new HiveContext(spark.sparkContext)
    val colorSchema = StructType(
      Array(StructField("color", StringType, true),
            StructField("frequency", LongType, true)))
    val colorDF = hiveContext.createDataFrame(skuColor, colorSchema).distinct
    loadParquetDataToHive(spark.sparkContext,
      colorDF.select("color"),
      "taxonomy_internal_sku_colors",
      args)

    val skuBrand = skuLog.select("brandname_en", "brandname_cn", "brandname_full")
      .flatMap { x =>
        Array(x(0).toString, x(1).toString, x(2).toString)
      }.distinct.select($"value".alias("brand")).filter("length(brand)>1")
    loadParquetDataToHive(spark.sparkContext, 
      skuBrand,
      "taxonomy_internal_sku_brands",
      args)

    val skuProduct = skuLog.select("product_words_list")
      .flatMap { x =>
        val getStringOrElse = (index: Int, value: String) => {
          (if (!x.isNullAt(index)) x.getString(index) else value)
        }
        getStringOrElse(0,"").toString.trim().replace(", ", ",").split(",")
      }.distinct.select($"value".alias("product_word")).filter("length(product_word)>1")
    loadParquetDataToHive(spark.sparkContext, 
      skuProduct,
      "taxonomy_internal_product_words",
      args)

    val stopWords = Seq("移动","联通","全网","电信","送","GB",
      "英寸","配","版","赠品","三网","套餐","适用").toDF("stopword")
    loadParquetDataToHive(spark.sparkContext,
      stopWords,
      "taxonomy_internal_stopwords",
      args)
  }
}
