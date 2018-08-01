package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object DeduplicateConcepts {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DeduplicateConcepts")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
    val log_object = new log.ConceptFromNaturalLog()
    val logRDD = log_object.getLogDF(spark.sparkContext, args)
                  .select("concept")
                  .rdd.map(_(0).toString)

    val rawConceptList = logRDD.collect.toSet
    val rawConceptIndex = logRDD.map { x =>
      (x(0), x)
    }.groupByKey().map { x =>
      (x._1, x._2.toSet)
    }.collect().toMap
    val filterRDD = checkConceptDuplicate(logRDD, rawConceptList, rawConceptIndex)
    val hiveContext = new HiveContext(spark.sparkContext)
    val schema = StructType(
      Array(StructField("concept", StringType, true)))
    val df = hiveContext.createDataFrame(filterRDD.map(Row(_)), schema)
    
    tools.LoadDataToHive.loadParquetDataToHive(
      spark.sparkContext, 
      df,
      "taxonomy_internal_sku_concepts_from_natural_deduplicated",
      args)
  }

  /*
    # sub_start_end_pos is a dict
    # the key is start index
    # the value is a set of end indices from different sub string (concept)
    # for example, the whole string is "abcdefg", two sub strings: "abc", "abcde", "bcd"
    # the dict will be: {0:{2,4}, 1:{3}}*/
  def checkConceptDuplicate(logRDD: RDD[String], rawConceptList: Set[String],
      rawConceptIndex: Map[Char, Set[String]]): RDD[String] = {
    logRDD.filter { concept =>
      var sub_start_end_pos: mutable.Map[Int, Set[Int]] = mutable.Map()
      val indexKeys = rawConceptIndex.keys
      for (c_key <- indexKeys) {
        if (concept.contains(c_key)) {
          for (c <- rawConceptIndex(c_key)) {
            if (c.length < concept.length) {
              val pos = concept.indexOf(c)
              if (pos >= 0) {
                if (sub_start_end_pos.contains(pos)) {
                  sub_start_end_pos(pos) = sub_start_end_pos(pos) + (pos+c.length-1)
                } else {
                  sub_start_end_pos(pos) = Set(pos+c.length-1)
                }
              }
            }
          }
        }
      }
      var result_set: mutable.Set[Boolean] = mutable.Set()
      !checkSubStringCoverage(0, sub_start_end_pos, concept.length, result_set)
    }
  }

  def checkSubStringCoverage(start: Int,
      sub_start_end_pos_dict: mutable.Map[Int, Set[Int]],
      string_len: Int, result_set: mutable.Set[Boolean]): Boolean = {
    var result_set_ = result_set
    if (start == string_len || result_set_.contains(true)) return true
    if (!sub_start_end_pos_dict.contains(start)) return false
    for (end <- sub_start_end_pos_dict(start)) {
      val result = checkSubStringCoverage(end+1, sub_start_end_pos_dict, string_len, result_set_)
      result_set_ += result
      //if result:
      //  break
    }
    if (result_set_.contains(true)) {
      return true
    } else {
      return false
    }
  }
}
