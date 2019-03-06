package com.jd.bgn.rules

import scala.collection.JavaConverters._
import scala.util.matching.Regex._
import scala.util.control._

import com.jd.bgn.utils.AttrValueUtil

class Common(
  columns: Array[String],
  remark_enable: Boolean,
  category_split: String,
  revision_bulk: String,
  supplement_min_fraction: Double,
  attr_value_length_mode: String,
  attr_value_omit: Array[String],
  attr_value_complement: Array[(String, String)],
  attr_value_min_proprttion: Double,
  rule: String
) extends Serializable {
  private var attr_value_scarce = Array[String]()
  def addAttrValueScarce(attr_value: String, attr_value_proprttion: Double): Unit = {
    if (attr_value_proprttion < attr_value_min_proprttion) {
      attr_value_scarce = attr_value_scarce :+ attr_value
    }
  }
  def getAttrValueScarce(): Array[String] = attr_value_scarce
  def getAttrValueComplement(): Array[(String, String)] = attr_value_complement
  def getRule: String = rule
  def getRemarkEnable: Boolean = remark_enable
  def getColumns: Array[String] = columns
  def filterValid(com_attr_value_cd: String, com_attr_value_name: String): Boolean = {
    com_attr_value_cd != null && 
    com_attr_value_cd.trim != "" &&
    com_attr_value_name != null &&
    com_attr_value_name.trim != "" &&
    !com_attr_value_name.contains("其他") &&
    !com_attr_value_name.contains("其它") &&
    !attr_value_omit.contains(com_attr_value_name) &&
    !attr_value_scarce.contains(com_attr_value_name)
  }
  def filterRevision(com_attr_value_cd: String, com_attr_value_name: String): Boolean = revision_bulk match {
    case "invalid" => !filterValid(com_attr_value_cd, com_attr_value_name)
    case "all" => true
    case "cleaning" => filterValid(com_attr_value_cd, com_attr_value_name)
    case _ => throw new Error(s"unknown revision bulk ${revision_bulk}")
  }
  def filterRequiredAttr(fraction: Double): Boolean = {
    if (fraction >= supplement_min_fraction) true else false
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
  def applyAttrValueRem(attr_value_rem: String, overall_attr_value: Array[(String, String, String)]): (String, String) = {
    var hit: (String, String, String) = null
    if (!remark_enable || attr_value_rem == null || attr_value_rem == "") {
      (null, null)
    } else {
      val loop = new Breaks
      loop.breakable {
        for (attr_value <- overall_attr_value) {
          if (("(?i)^.{0,1}" + attr_value._3 + ".{0,1}$").r.findFirstIn(attr_value_rem) != None) {
            hit = attr_value
            loop.break
          }
        }
      }
      if (hit == null) (null, null) else (hit._1, hit._2)
    }
  }
}