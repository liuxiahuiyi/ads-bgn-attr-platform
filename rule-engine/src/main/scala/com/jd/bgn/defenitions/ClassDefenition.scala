package com.jd.bgn.defenitions
import scala.collection.JavaConverters._
import java.util.Properties

case class Config(
  date: String,
  target_db_table: String,
  source_id: String,
  repartition: Int,
  use_local_attr_set: Boolean
) extends Serializable

case class Source(
  item_first_cate_cd: String,
  item_second_cate_cd: String,
  item_third_cate_cd: String,
  item_sku_id: String,
  sku_name: String,
  barndname_full: String,
  colour: String,
  size: String,
  jd_prc: String,
  com_attr_cd: String,
  com_attr_name: String,
  com_attr_value_cd: String,
  com_attr_value_name: String,
  com_attr_value_rem: String,
  com_attr_group: String
) extends Serializable

case class Target(
  item_first_cate_cd: String,
  item_second_cate_cd: String,
  item_third_cate_cd: String,
  item_sku_id: String,
  sku_name: String,
  barndname_full: String,
  colour: String,
  size: String,
  jd_prc: String,
  com_attr_cd: String,
  com_attr_name: String,
  com_attr_value_cd: String,
  com_attr_value_name: String,
  com_attr_value_rem: String,
  com_attr_group: String,
  alt_attr_value_cd: String,
  alt_attr_value_name: String,
  old_attr_value_cd: String,
  old_attr_value_name: String,
  source_id: String
) extends Serializable {
  def getMeta(column: String) = column match {
    case "sku_name" => sku_name
    case "barndname_full" => barndname_full
    case "colour" => colour
    case "size" => size
    case "jd_prc" => jd_prc
    case "_" => throw new Error(s"unknown meta column ${column}")
  }
}

