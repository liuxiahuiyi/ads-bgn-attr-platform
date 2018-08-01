package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.jd.bgn.CommonArgs

trait LogFactory extends Serializable {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame
}
