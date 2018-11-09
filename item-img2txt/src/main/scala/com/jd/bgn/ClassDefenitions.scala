package com.jd.bgn.item_img2txt

import scala.collection.JavaConverters._
import org.joda.time.DateTime

case class Config(
  date: String,
  target_db_table: String,
  library_hdfs: String,
  ocr_trained_data_hdfs: String,
  item_first_cate_cds: String
) extends Serializable {
  def calDate(nday: Int): String = {
    val dt = new DateTime(date)
    dt.plusDays(nday)
      .toString("yyyy-MM-dd")
  }
  def getFirstCateCondition(): String = {
    if (item_first_cate_cds == "all") {
      " "
    } else {
      val item_first_cate_cd_str = item_first_cate_cds.split(",")
                                                      .filter(_ != "")
                                                      .map("'" + _ + "'")
                                                      .mkString("(", "," ,")")
      s" and item_first_cate_cd in ${item_first_cate_cd_str} "
    }
  }
}

case class Target(
  item_first_cate_cd: String,
  item_id: String,
  item_img_url: String,
  item_img_txt: String,
  start_date: String
) extends Serializable
