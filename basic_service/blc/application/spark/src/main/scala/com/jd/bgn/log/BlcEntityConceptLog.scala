package com.jd.bgn.log
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext 
import org.apache.spark.sql.DataFrame 
import java.text.SimpleDateFormat
import com.jd.bgn.CommonArgs

class BlcEntityConceptLog extends LogFactory {
  def getLogDF(sc: SparkContext, args: CommonArgs): DataFrame = { 
    val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    val hiveContext = new HiveContext(sc)
    val query = s"""
    select entity_id,
    entity_name,
    concept_id,
    concept_name,
    score,
    algorithm
    from ad_bgn.blc_entity_concept_v2
    where dt = '${dateStr}'
    and category='${args.partitionCategory}'
    and algorithm='"AllDimConcepts"'
    """
    hiveContext.sql(query)
  }
}
