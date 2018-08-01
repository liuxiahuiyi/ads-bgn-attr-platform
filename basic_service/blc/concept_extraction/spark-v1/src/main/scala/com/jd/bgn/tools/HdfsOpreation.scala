package com.jd.bgn.tools

object HdfsOpreation {
  def deleteHdfsPath(filePath: String) = {
    val prefixSet = Set("hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db")
    if (!filePath.matches("(" + prefixSet.mkString("|") + ").*")) {
      System.err.println(
        s"Only path that starts with '${prefixSet.mkString(",")}' " +
        "can be delete FOR SAFETY REASON.")
    } else {
      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      try {
        hdfs.delete(new org.apache.hadoop.fs.Path(filePath), true)
      } catch {
        case _ : Throwable => { }
      }
    }
  }

  def checkHdfsPath(filePath: String): Boolean = {
    if (!filePath.startsWith("hdfs://")) {
      System.err.println("hdfs file path illegal")
      System.exit(1)
    }
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val uri = filePath.take(filePath.indexOf("/", 7))
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
        new java.net.URI(uri), hadoopConf)
    hdfs.exists(new org.apache.hadoop.fs.Path(filePath))
  }

  def createHdfsPath(filePath: String) = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.create(new org.apache.hadoop.fs.Path(filePath), true)
    } catch {
      case _ : Throwable => { }
    }
  }
}
