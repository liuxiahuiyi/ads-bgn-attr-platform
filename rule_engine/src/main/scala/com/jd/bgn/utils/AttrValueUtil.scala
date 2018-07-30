package com.jd.bgn.utils

import scala.collection.JavaConverters._
import scala.util.matching.Regex._

object AttrValueUtil extends Serializable {
  def split(value: String): Array[String] = {
    val splited = "[&/(（)）]+".r.split(value)
                                .filter((e) => e != null && e.trim != "")
                                .map(_.trim)
    if (splited.length > 1) {
      splited
    } else {
      val reg1 = "([\u4e00-\u9fa5 ]+) +([\\w ]+)".r
      val reg2 = "([\\w ]+) +([\u4e00-\u9fa5 ]+)".r
      value match {
        case reg1(part1, part2) => Array(part1, part2)
        case reg2(part1, part2) => Array(part1, part2)
        case _ => Array(value)
      }
    }
  }
  def length(value: String, mode: String): Int = {
    mode match {
      case "word" => "[^\u4e00-\u9fa5]".r.replaceAllIn(value, "").length + "[\u4e00-\u9fa5 ]+".r.split(value).filter(_ != "").length
      case "char" => value.length
      case _ => throw new Error(s"unknown length mode ${mode}")
    }
  }
  def getPattern(value: String, sep: String): String = {
    value.split("").filter(_ != "").mkString(sep)
  }
  def subString(source: String, start: Int, end: Int, mode: String): String = {
    mode match {
      case "word" => {
        val part1 = "[\u4e00-\u9fa5 ]+".r.split(source)
        val part2 = "[\u4e00-\u9fa5 ]+".r.findAllIn(source)
        var parts = Array[String]()
        for (e <- part1) {
          parts = parts :+ e
          if (part2.hasNext) {
            parts = parts ++ part2.next.split("").filter((e) => e != "" && e != " ")
          }
        }
        if (part2.hasNext) {
          parts = parts ++ part2.next.split("").filter((e) => e != "" && e != " ")
        }
        parts.filter(_ != "").slice(start, end).mkString
      }
      case "char" => source.substring(start, end)
      case _ => throw new Error(s"unknown mode ${mode}")
    }
  }
}