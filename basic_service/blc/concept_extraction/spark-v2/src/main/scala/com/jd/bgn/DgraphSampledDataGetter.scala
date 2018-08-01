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

object DgraphSampledDataGetter {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DgraphSampledDataGetter")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val args = new CommonArgs(arguments)
   
    val sampledDF = getSampledDF(spark, args.partitionCategory)
    val log1 = sampledDF.select("item_sku_id", "sku_name")
    val log2 = sampledDF.select("item_sku_id", "entity_id", "entity")

    val log3 = (new log.GraphConceptHashLog()).getLogDF(spark.sparkContext, args)
    val log4 = (new log.GraphEntityConceptEdgeLog()).getLogDF(spark.sparkContext, args)
    val log5 = (new log.GraphConceptConceptEdgeLog()).getLogDF(spark.sparkContext, args)
    val log6 = (new log.BlcEntityConceptLog()).getLogDF(spark.sparkContext, args)
    val log7 = (new log.RelationLinkingNameEntityLog()).getLogDF(spark.sparkContext, args)

    val data = (
    log1.selectExpr(
        "concat('<sku_',item_sku_id,'> <id> \"',item_sku_id,'\" .')",
        "concat('<sku_',item_sku_id,'> <name> \"',regexp_replace(regexp_replace(sku_name,'\"',''), '\01',''),'\" .')")
      .unionAll(
        log2.select("entity_id", "entity").distinct.selectExpr(
          "concat('<',entity_id,'> <id> \"',entity_id,'\" .')",
          "concat('<',entity_id,'> <name> \"',entity,'\" .')"))
      .unionAll(
        log3.selectExpr(
          "concat('<',concept_id,'> <id> \"',concept_id,'\" .')",
          "concat('<',concept_id,'> <name> \"',regexp_replace(concept_name,'\"',''),'\" .')"))
      .rdd.flatMap{ x => Array(Row(x(0).toString), Row(x(1).toString))}
    ).union(
      log7.selectExpr(
        "concat('<',linking_name_code,'> <id> \"',linking_name_code,'\" .')",
        "concat('<',linking_name_code,'> <linking_name> \"',linking_name,'\" .')",
        "concat('<',linking_name_code,'> <containsEntity> <',linking_entity,'> .')")
      .rdd.flatMap{ x => Array(Row(x(0).toString), Row(x(1).toString), Row(x(2).toString))}
    ).union(
    log2.selectExpr("concat('<sku_',item_sku_id,'> <isA> <',entity_id,'> .')")
      .unionAll(log4.selectExpr("concat('<',entity,'> <isA> <',concept,'> .')"))
      .unionAll(log5.selectExpr("concat('<',son,'> <isA> <',father,'> .')"))
      .unionAll(log6.selectExpr("concat('<',entity_id,'> <isBLC> <',concept_id,'> .')"))
      .rdd
    ).map {_(0).toString}

    tools.LoadDataToHive.loadLineDataToHive(
      spark.sparkContext,
      data.repartition(10),
      "dgraph_triad_rdf",
      args)
  }

  def getSampledDF(spark: SparkSession, category: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val filePath = "guokun/dgraph/"+category
    sc.textFile(filePath).map { e =>
      val segs = e.split("\t")
      (segs(0), segs(1), segs(2), segs(3))
    }.toDF("item_sku_id", "sku_name", "entity_id", "entity")
  } 
}
