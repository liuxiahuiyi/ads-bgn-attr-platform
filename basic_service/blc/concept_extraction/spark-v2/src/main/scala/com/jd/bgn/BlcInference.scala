package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable


object BlcInference {

  def findBlcTopK(conceptSet: Set[(String, String, Int, Int, Int)], k: Int, nInstance: Long) : List[Tuple3[String, String, Double]] = {
    val epsilon = 0.00001
    var sigmaNCiE = conceptSet.map { conceptInfo =>
      conceptInfo._4
    }.sum
    var blcNameValue: List[Tuple3[String, String, Double]] = List()
    conceptSet.foreach { conceptInfo =>
      var nce = conceptInfo._4
      var sigmaNCEi = conceptInfo._3
      var pec = (nce + epsilon) / (sigmaNCEi + epsilon*nInstance)
      var pce = (nce + epsilon) / (sigmaNCiE + epsilon*nInstance)
      var repEC = pce * pec
      blcNameValue :+= (conceptInfo._1, conceptInfo._2, repEC)
    }
    var blcNameValueSorted = blcNameValue.sortWith((s,t) => s._3>t._3)
    var takeLength = if (k < blcNameValueSorted.length) k else blcNameValueSorted.length
    var result = blcNameValueSorted.take(takeLength)
    return result
  }

  def main(arguments: Array[String]) {
    val args = new CommonArgs(arguments)
    val spark = SparkSession
      .builder()
      .appName("BlcInference")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)    
    val hiveContext = new HiveContext(spark.sparkContext)
    val getSkuNumSql = s"""
      select count(*) 
        from ad_bgn.ad_bgn_sku_entity
      where dt = '${dateStr}'    
        and category='${args.partitionCategory}'
    """

    val getEntityConceptWeightSql = s"""
      select entity as entity_id, concept as concept_id, weight
        from ad_bgn.graph_entity_concept_weight
      where dt = '${dateStr}'    
        and category='${args.partitionCategory}'
    """

    val getEntityNameSql = s"""
      select distinct entity_id as entity_id_raw, entity as entity_name
        from ad_bgn.ad_bgn_sku_entity
      where dt = '${dateStr}'    
        and category='${args.partitionCategory}'
    """

    val getConceptNameSql = s"""
      select concept_id as concept_id_raw, concept_name, concept_level
        from ad_bgn.graph_concept_hash_v2
      where dt = '${dateStr}'    
        and category='${args.partitionCategory}'
    """

    val nInstance = hiveContext.sql(getSkuNumSql).collect()(0).getLong(0)
    val entityConceptWeightDF = hiveContext.sql(getEntityConceptWeightSql)
    val entityNameDF = hiveContext.sql(getEntityNameSql)
    val conceptNameDF = hiveContext.sql(getConceptNameSql)

    val conceptWeightSum = entityConceptWeightDF.groupBy("concept_id")
                           .agg(Map("weight" -> "sum"))
                           .select($"concept_id".alias("concept_for_weight"), $"sum(weight)".alias("sum_weight"))

    val wholeTable = entityConceptWeightDF.join(entityNameDF, entityConceptWeightDF("entity_id")===entityNameDF("entity_id_raw"), "left_outer")
                         .join(conceptNameDF, entityConceptWeightDF("concept_id")===conceptNameDF("concept_id_raw"), "left_outer")
                         .join(conceptWeightSum, entityConceptWeightDF("concept_id")===conceptWeightSum("concept_for_weight"), "left_outer")
                         .select("entity_id", "entity_name", "concept_id", "concept_name", "sum_weight", "weight", "concept_level")
                         .rdd.map { x =>
                           ((x(0).toString, x(1).toString), (x(2).toString, x(3).toString, x(4).toString.toInt, x(5).toString.toInt, x(6).toString.toInt))
                         }.groupByKey().map { x =>
                           (x._1, x._2.toSet)
                         }
//    println(wholeTable.count)
//    println(wholeTable.take(10).foreach(println))
    val blc = wholeTable.flatMap { x =>
      val entity_id = x._1._1
      val entity_name = x._1._2
      val conceptSet = x._2
      var oneDimConceptSet: Set[(String, String, Int, Int, Int)] = Set()
      conceptSet.foreach { conceptInfo =>
        if (conceptInfo._5 == 1) {
          oneDimConceptSet += conceptInfo
        }
      }
      var blcSeq: Seq[String]= Seq()
      val topKBlc = findBlcTopK(conceptSet, 10, nInstance)
      val topKBlcOneDim = findBlcTopK(oneDimConceptSet, 10, nInstance)

      topKBlc.foreach { x =>
        val concept_id = x._1
        val concept_name = x._2
        val score = x._3
//        blcSeq :+= Row((entity_id.toString, entity_name.toString, concept_id.toString, concept_name.toString, score.toString.toDouble, "AllDimConcepts".toString)
        blcSeq :+= s"""${entity_id}\t${entity_name}\t${concept_id}\t${concept_name}\t${score}\t"AllDimConcepts""""
      }
      topKBlcOneDim.foreach { x =>
        val concept_id = x._1
        val concept_name = x._2
        val score = x._3
//        blcSeq :+= Row(entity_id.toString, entity_name.toString, concept_id.toString, concept_name.toString, score.toString.toDouble, "OneDimConcepts".toString)
        blcSeq :+= s"""${entity_id}\t${entity_name}\t${concept_id}\t${concept_name}\t${score}\t"OneDimConcepts""""
      }
      blcSeq
    }

//    println(blc.take(10).foreach(println))
//    println(blc.count)
//
//
//    val schema = StructType(
//      List(StructField("entity_id", StringType, true),
//        StructField("entity_name", StringType, true),
//        StructField("concept_id", StringType, true),
//        StructField("concept_name", StringType, true),
//        StructField("score", DoubleType, true),
//        StructField("algorithm", StringType, true)))


//    val blcDF = hiveContext.createDataFrame(blc.map(Row(_)), schema)
//    val blcDF = hiveContext.createDataFrame(blc, schema)

//    tools.LoadDataToHive.loadParquetDataToHive(
//      spark.sparkContext,
//      blcDF,
//      "blc_entity_concept",
//      args) 
    tools.LoadDataToHive.loadLineDataToHive(
      spark.sparkContext,
      blc,
      "blc_entity_concept_v2",
      args) 
  }
}
