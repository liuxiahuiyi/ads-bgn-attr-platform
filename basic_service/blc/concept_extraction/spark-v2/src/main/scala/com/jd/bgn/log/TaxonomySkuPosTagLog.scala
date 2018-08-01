package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class TaxonomySkuPosTagLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select skuid,
      skuname,
      pos_tag
    from ad_bgn.taxonomy_sku_pos_tag
    where dt = '${dateStr}'
    and category='${args.partitionCategory}'
    """
    hiveContext.sql(query)
  }
}
