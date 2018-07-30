package com.jd.bgn.rules

import scala.collection.JavaConverters._
import scala.util.matching.Regex._
import scala.util.control._

class Range() extends Serializable {
  def preprocess(attr_value_name: String): Array[String] = {
    val region = "-+".r.split(attr_value_name)
                       .map("\\d+\\.?\\d*".r.findFirstIn(_))
                       .filter(_ != None)
                       .map(_.get)
    var preprocessed = Array[String]()
    if (region.length > 1) {
      preprocessed = region.slice(0, 2)
    } else if (region.length == 1){
      if (attr_value_name.contains("上")) {
        preprocessed = Array(region(0), "9999999999")
      }
      if (attr_value_name.contains("下")) {
        preprocessed = Array("0", region(0))
      }
    }
    preprocessed
  }
  def process(metadatas: Array[String], preprocessed_attr_value_set: Array[(String, String, Long, Array[String])]): (String, String) = {
    var hit = Array[(String, String, Long, Array[String])]()
    for (metadata <- metadatas if metadata != null && metadata != "" && "\\d+\\.?\\d*".r.findFirstIn(metadata) != None) {
      val price = "\\d+\\.?\\d*".r.findFirstIn(metadata).get
      val loop = new Breaks
      loop.breakable {
        for (preprocessed_attr_value <- preprocessed_attr_value_set) {
          if (preprocessed_attr_value._4.length >= 2
              && preprocessed_attr_value._4(0).toDouble <= price.toDouble
              && preprocessed_attr_value._4(1).toDouble >= price.toDouble) {
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