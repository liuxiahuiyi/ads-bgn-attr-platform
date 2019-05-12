package com.jd.bgn.rule_engine

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkEnv extends Serializable {

  @transient private val conf = new SparkConf
  @transient private val session = SparkSession.builder()
                                               .appName("bgn_rule_engine")
                                               .config(this.conf)
                                               .enableHiveSupport()
                                               .getOrCreate()

  /**
    * Returns Spark configuration
    * <b>Caution</b>: never try to change or add to the configuration during lifetime of this application. Keep it
    * immutable across the project.
    *
    * @return globally unique Spark configuration
    */
  def getConf: SparkConf = this.conf

  /**
    * Returns Spark session
    * <b>Caution</b>: never try to stop the session, i.e., invoke [[SparkSession.stop()]], during lifetime of this
    * application, since the session is being shared across the classes and objects.
    *
    * @return globally unique Spark session
    */
  def getSession: SparkSession = this.session

}
