package com.jd.bgn.tools
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import com.jd.bgn.CommonArgs
import org.apache.spark.sql.types._

object LoadDataToHive {
  def loadParquetDataToHive(sc: SparkContext, df: DataFrame,
      table: String, args: CommonArgs) {
    val hiveContext = new HiveContext(sc)
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    df.withColumn("dt", lit(dateStr))
      .withColumn("category", lit(args.partitionCategory))
      .write.mode("overwrite").format("parquet")
      .save(args.hivePath+table+"/dt="+dateStr+"/category="+args.partitionCategory)
    val addPartitionSqlString = s"""
      alter table ad_bgn.%s add if not exists
      partition(dt='%s', category='%s')
    """.format(table, dateStr, args.partitionCategory)
    hiveContext.sql(addPartitionSqlString)
  }

  def loadLineDataToHive(sc: SparkContext, rdd: RDD[String],
      table: String, args: CommonArgs) {
    val hiveContext = new HiveContext(sc)
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val path = args.hivePath+table+"/dt="+dateStr+"/category="+args.partitionCategory
    HdfsOpreation.deleteHdfsPath(path)
    rdd.saveAsTextFile(path)
    val addPartitionSqlString = s"""
      alter table ad_bgn.%s add if not exists
      partition(dt='%s', category='%s')
    """.format(table, dateStr, args.partitionCategory)
    hiveContext.sql(addPartitionSqlString)
  }
}
