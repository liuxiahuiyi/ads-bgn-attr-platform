package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class SkuAttributeLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select item_sku_id,
      sku_name,
      brandname_en,
      brandname_cn,
      brandname_full,
      itemtype_title,
      product_words_list,
      best_product,
      spec_attr,
      ext_attr
    from ad_bgn.ad_bgn_sku_attribute
    where dt = '${dateStr}'
    and category='${args.partitionCategory}'
    """
    hiveContext.sql(query)
  }
}
