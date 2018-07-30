package com.jd.bgn.rules

import scala.collection.JavaConverters._
import scala.util.matching.Regex._

import com.jd.bgn.utils.AttrValueUtil

class Matching(
  length_mode: String,
  context_left: String,
  context_right: String,
  tolerance: Int,
  length_difference_tolerance: Int,
  custom: Map[String, String]
) extends Serializable {
  def preprocess(attr_value_name: String): Array[String] = {
    var preprocessed = Array[String]()
    val split_attr_value_name = AttrValueUtil.split(attr_value_name)
                                             .map(_.replaceAll("\\pP|\\pS", ""))
    if (custom.contains(attr_value_name) && custom(attr_value_name) != null && custom(attr_value_name) != "") {
      preprocessed = preprocessed :+ custom(attr_value_name)
    } else if (split_attr_value_name.length == 1) {
      val first_element = split_attr_value_name(0)
      if ("[^\u4e00-\u9fa5]".r.replaceAllIn(first_element, "").length > 1 || first_element.length > 2) {
        preprocessed = preprocessed :+ AttrValueUtil.getPattern(first_element, s".{0,${tolerance}}")
      }
    } else if (split_attr_value_name.length > 1) {
      val first_element = split_attr_value_name(0)
      val second_element = split_attr_value_name(1)
      val first_element_length = AttrValueUtil.length(first_element, length_mode)
      val second_element_length = AttrValueUtil.length(second_element, length_mode)
      val length_difference = first_element_length - second_element_length
      if (length_difference > length_difference_tolerance) {
        preprocessed = preprocessed :+ AttrValueUtil.getPattern(first_element, s".{0,${tolerance}}")
        val mid = AttrValueUtil.subString(first_element, 0, length_difference / 2, length_mode) + second_element
        preprocessed = preprocessed :+ AttrValueUtil.getPattern(mid, s".{0,${tolerance}}")
      } else if (length_difference < -length_difference_tolerance) {
        preprocessed = preprocessed :+ AttrValueUtil.getPattern(second_element, s".{0,${tolerance}}")
        val mid = first_element + AttrValueUtil.subString(second_element, first_element_length - length_difference / 2, second_element_length, length_mode)
        preprocessed = preprocessed :+ AttrValueUtil.getPattern(element_pattern, s".{0,${tolerance}}")
      } else {
        if ("[^\u4e00-\u9fa5]".r.replaceAllIn(first_element, "").length > 1 || first_element.length > 2) {
          preprocessed = preprocessed :+ AttrValueUtil.getPattern(first_element, s".{0,${tolerance}}")
        }
        if ("[^\u4e00-\u9fa5]".r.replaceAllIn(second_element, "").length > 1 || second_element.length > 2) {
          preprocessed = preprocessed :+ AttrValueUtil.getPattern(second_element, s".{0,${tolerance}}")
        }
      }
    }
    preprocessed
  }
  def process(metadatas: Array[String], preprocessed_attr_value_set: Array[(String, String, Int, Array[String])]): (String, String) = {
    var hit = Array[(String, String, Int, Array[String])]()
    for (metadata <- metadatas if metadata != null && metadata != "") {
      val loop = new Breaks
      loop.breakable {
        for (preprocessed_attr_value <- preprocessed_attr_value_set) {
          for (e <- preprocessed_attr_value._4) {
            if (s"(?i)${context_left}${e}${context_right}".r.findFirstIn(metadata) != None) {
              hit = hit :+ preprocessed_attr_value
              loop.break
            }
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