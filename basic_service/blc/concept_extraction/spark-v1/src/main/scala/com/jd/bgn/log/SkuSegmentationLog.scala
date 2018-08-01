package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class SkuSegmentationLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select sku_id,
    sku_segment
    from ad_bgn.taxonomy_sku_segmentation
    where dt = '${dateStr}'
    and category='${args.partitionCategory}'
    """
    hiveContext.sql(query)
  }
}
