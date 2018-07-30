package com.jd.bgn.rules

import scala.collection.JavaConverters._
import scala.util.matching.Regex._

import com.jd.bgn.utils.AttrValueUtil

class Common(
  columns: Array[String],
  category_split: String,
  attr_value_length_mode: String,
  attr_value_omit: Array[String],
  rule: String
) extends Serializable {
  def getRule: String = rule
  def getColumns: Array[String] = columns
  def filter(com_attr_value_cd: String, com_attr_value_name: String): Boolean = {
    com_attr_value_cd != null && 
    com_attr_value_cd.trim != "" &&
    com_attr_value_name != null &&
    com_attr_value_name.trim != "" &&
    !com_attr_value_name.contains("其他") &&
    !com_attr_value_name.contains("其它") &&
    !attr_value_omit.contains(com_attr_value_name)
  }
  def getCateKey(item_first_cate_cd: String, item_second_cate_cd: String, item_third_cate_cd: String): (String, String, String) = category_split match {
    case "item_first_cate_cd" => (item_first_cate_cd, null, null)
    case "item_second_cate_cd" => (item_first_cate_cd, item_second_cate_cd, null)
    case "item_third_cate_cd" => (item_first_cate_cd, item_second_cate_cd, item_third_cate_cd)
    case _ => throw new Error(s"unknown category split ${category_split}")
  }

  def sort(value_a: (String, String, Long), value_b: (String, String, Long)): Boolean = {
    val al = AttrValueUtil.split(value_a._2)
                          .map(AttrValueUtil.length(_, attr_value_length_mode))
                          .max
    val bl = AttrValueUtil.split(value_b._2)
                          .map(AttrValueUtil.length(_, attr_value_length_mode))
                          .max
    al > bl || (al == bl && value_a._3 > value_b._3)
  }
}