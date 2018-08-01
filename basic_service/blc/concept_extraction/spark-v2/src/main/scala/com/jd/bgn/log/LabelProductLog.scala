package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class LabelProductLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select user_account,
    skuid,
    skuname,
    label,
    check_result,
    check_result_raw,
    check_person
    from ad_bgn.label_product
    where category='${args.partitionCategory}'
    """
    hiveContext.sql(query)
  }
}
