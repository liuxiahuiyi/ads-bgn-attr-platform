package com.jd.bgn.rules

import scala.collection.JavaConverters._
import scala.util.matching.Regex._
import scala.util.control._

import com.jd.bgn.utils.AttrValueUtil

class Age() extends Serializable {
  private def transformToYear(number: String, unit: String): String = {
    unit match {
      case "岁" => number
      case "月" => (number.toDouble / 12).toString
      case _ => throw new Error(s"unkonwn age unit ${unit}")
    }
  }
  private val reg1 = "(?:^\\D*|^.*\\D{4})(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁]).{0,6}?(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁])(?:\\D*$|\\D{4}.*$)".r
  private val reg2 = "(?:^\\D*|^.*\\D{4})(\\d+|\\d+\\.\\d+).{0,6}?(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁])(?:\\D*$|\\D{4}.*$)".r
  private val reg3 = "(?:^\\D*|^.*\\D{4})(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁])\\D{0,4}(上)(?:\\D*$|\\D{4}.*$)".r
  private val reg4 = "(?:^\\D*|^.*\\D{4})(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁])\\D{0,4}(下)(?:\\D*$|\\D{4}.*$)".r
  private val reg5 = "(?:^\\D*|^.*\\D{4})(\\d+|\\d+\\.\\d+)\\D{0,2}([月岁])(?:\\D*$|\\D{4}.*$)".r
  def preprocess(attr_value_name: String): Array[String] = {
    attr_value_name match {
      case reg1(num1, u1, num2, u2) => Array(transformToYear(num1, u1), transformToYear(num2, u2))
      case reg2(num1, num2, u) => Array(transformToYear(num1, u), transformToYear(num2, u))
      case reg3(num, u, d) => Array(transformToYear(num, u), "100")
      case reg4(num, u, d) => Array("0", transformToYear(num, u))
      case _ => Array(AttrValueUtil.getPattern(attr_value_name, ".{0,2}"))
    }
  }
  def process(metadatas: Array[String], preprocessed_attr_value_set: Array[(String, String, Long, Array[String])]): (String, String) = {
    var hit = Array[(String, String, Long, Array[String])]()
    for (metadata <- metadatas if metadata != null && metadata != "") {
      val (lv, rv) = metadata match {
        case reg1(num1, u1, num2, u2) => (transformToYear(num1, u1), transformToYear(num2, u2))
        case reg2(num1, num2, u) => (transformToYear(num1, u), transformToYear(num2, u))
        case reg3(num, u, d) => (transformToYear(num, u), (transformToYear(num, u).toDouble + 2).toString)
        case reg4(num, u, d) => ((transformToYear(num, u).toDouble - 2).toString, transformToYear(num, u))
        case reg5(num, u) => (transformToYear(num, u), transformToYear(num, u))
        case _ => (null, null)
      }
      val loop = new Breaks
      loop.breakable {
        for (preprocessed_attr_value <- preprocessed_attr_value_set) {
          if (preprocessed_attr_value._4.length >= 2
              && lv != null
              && rv != null
              && Math.abs(preprocessed_attr_value._4(0).toDouble - lv.toDouble) <= 1
              && Math.abs(preprocessed_attr_value._4(1).toDouble - rv.toDouble) <= 1) {
            hit = hit :+ preprocessed_attr_value
            loop.break
          }
          if (preprocessed_attr_value._4.length == 1
              && s"(?i)${preprocessed_attr_value._4(0)}".r.findFirstIn(metadata) != None) {
            hit = hit :+ preprocessed_attr_value
            loop.break
          }
        }
      }
    }
    if (hit.length == metadatas.length && hit.toSet.size == 1) {
      (hit(0)._1, hit(0)._2)
    } else {
      (null, null)
    }
  }
}