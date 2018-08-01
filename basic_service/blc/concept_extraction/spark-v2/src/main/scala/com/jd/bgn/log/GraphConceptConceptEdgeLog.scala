package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class GraphConceptConceptEdgeLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select son,
    father
    from ad_bgn.graph_concept_concept_edge
    where dt = '${dateStr}'
    and category='${args.partitionCategory}'
    """
    hiveContext.sql(query)
  }
}
