package com.jd.bgn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.SimpleDateFormat
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact,render,parse}
import com.jd.bgn.tools.LoadDataToHive.loadLineDataToHive

object BlcResultGetter {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("BlcResultGetter")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val args = new CommonArgs(arguments)
    val log1 = new log.SkuEntityLog()
    val skuEntityLog = log1.getLogDF(sc, args)
      .select("entity_id", "item_sku_id")
      .rdd.map { x =>
        (x(0).toString, x(1).toString)
      }

    val log2 = new log.BlcEntityConceptLog()
    val blcLog = log2.getLogDF(sc, args)
      .select("entity_id", "concept_id", "score")
      .rdd.map { x =>
        val getDoubleOrElse = (row: Row, index : Int, value: Double) => {
          (if(!row.isNullAt(index)) row.getDouble(index) else value)
        }
        val getLongOrElse = (row: Row, index: Int, value: Long) => {
          (if (!row.isNullAt(index)) row.getLong(index) else value)
        }
        (x(0).toString, (Math.abs(x(1).toString.toLong),
          getDoubleOrElse(x, 2, 0.0)))
      }.groupByKey().map { x =>
        val concepts = x._2.toSet.toList.sortWith(_._2 > _._2).map(_._1)
        val json = ("blc_ids", concepts.take(3)) ~ ("concept_ids", concepts.take(10))
        (x._1, compact(render(json)))
      }

    val joinRDD = skuEntityLog.leftOuterJoin(blcLog).map { x =>
      (x._2._1, x._2._2.getOrElse(""))
    }.filter(_._2 != "").map { x =>
      s"""${x._1}\t70\t${x._2}"""
    }
    val path = "/user/jd_ad/ads_anti/yangxi/sku_service/" +
      new SimpleDateFormat("yyyy-MM-dd").format(args.date) +
      "/" + args.partitionCategory
    joinRDD.repartition(100).saveAsTextFile(path)
  }
}
