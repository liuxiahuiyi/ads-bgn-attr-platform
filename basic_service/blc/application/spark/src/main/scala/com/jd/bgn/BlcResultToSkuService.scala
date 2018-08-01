package com.jd.bgn

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Calendar

object BlcResultToSkuService {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("BlcResultToSkuService")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val args = new CommonArgs(arguments)
    var cal = Calendar.getInstance
    cal.setTime(args.date)
    cal.add(Calendar.DATE, -1)

    val inputPath = "/user/jd_ad/ads_anti/yangxi/sku_service/" +
      new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime) +
      "/*/*"
    val outputPath = "/user/jd_ad/index_resource/ad_sku_offline_data/sku/sku_attr_field_data/daily/" + 
      new SimpleDateFormat("yyyy-MM-dd").format(args.date) +
      "/bgn_info_field/"
    val data = sc.textFile(inputPath).repartition(100).saveAsTextFile(outputPath)
  }
}
