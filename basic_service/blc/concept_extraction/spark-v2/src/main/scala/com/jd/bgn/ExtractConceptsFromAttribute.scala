package com.jd.bgn

import org.apache.spark.sql.SparkSession
import com.jd.bgn.tools.LoadDataToHive.loadLineDataToHive
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact,render,parse}

object ExtractConceptsFromAttribute {
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("ExtractConceptsFromAttribute")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val args = new CommonArgs(arguments)
    val log1 = new log.SkuAttributeLog()
    val skuLog = log1.getLogDF(sc, args)

    val attrLog= skuLog.select("item_sku_id","sku_name","best_product",
      "brandname_en","brandname_cn","brandname_full","product_words_list",
      "spec_attr", "ext_attr")
      .rdd.map { x =>
        val getStringOrElse = (index: Int, value: String) => {
          (if (!x.isNullAt(index)) x.getString(index) else value)
        }
        (x(0).toString, (x(1).toString,
          getStringOrElse(2, ""), getStringOrElse(3, ""),
          getStringOrElse(4, ""), getStringOrElse(5, ""),
          getStringOrElse(6, "").trim().replace(", ", ","),
          x(7).asInstanceOf[Map[String, String]],
          x(8).asInstanceOf[Map[String, String]]))
      }
    val log2 = new log.TaxonomySkuPosTagLog()
    val posTagLog = log2.getLogDF(sc, args).select("skuid","pos_tag")
      .rdd.map { x =>
        (x(0).toString, x(1).toString)
      }
    val needAttrs = Set("功能属性","材质属性","样式属性","风格属性",
      "人群属性", "场景属性","季节属性","地区属性","味道属性")
    val concept = attrLog.leftOuterJoin(posTagLog).flatMap { x =>
      val skuid = x._1
      val skuname = x._2._1._1.replace("\01", "")
      val best_product = x._2._1._2
      val brands = Set(x._2._1._3, x._2._1._4, x._2._1._5).filter(_.size > 1)
      val products = x._2._1._6.split(",").toSet.filter(_.size > 1)
      val spec_attr =  x._2._1._7
      val ext_attr = x._2._1._8
      val pos_tag = x._2._2.get

      if (best_product != "" && best_product != "Null") {
        //从title中抽取概念
        val titleConcepts = extractTitleConcept(pos_tag).filter(x =>
          titleConceptFilter(x._1, x._2, products, brands, needAttrs))
        val titleConceptJson = makeTitleConceptJson(titleConcepts)

        //从扩展属性中抽取概念
        val titleAttrs = titleConcepts.map(_._1).toSet
        val titleWords = titleConcepts.map(_._2).toSet.mkString(",")
        var extConceptJson = "{}"
        if (titleAttrs.size <= 1) {
          val needExtAttrs = needAttrs -- titleAttrs
          val extConcepts =
            (extractExtConcept(spec_attr, products, brands, titleWords) ++
            extractExtConcept(ext_attr, products, brands, titleWords))
            .take(needExtAttrs.size)
          extConceptJson = makeExtConceptJson(extConcepts) 
        }
        if (titleConceptJson != "{}" || extConceptJson != "{}") {
          Some(s"""${skuid}\01${skuname}\01${titleConceptJson}\01${extConceptJson}\01${best_product}""")
        } else {
          None
        }
      } else {
        None
      }
    }
    loadLineDataToHive(sc, concept.repartition(10),
        "taxonomy_sku_concept_product", args)
  }

  def extractTitleConcept(pos_tag: String): List[(String, String, String)] = {
    parse(pos_tag, false)
      .values.asInstanceOf[List[Map[String, Any]]]
      .map { e =>
        val label = e.get("label").get.toString
        val word = e.get("word").get.toString
        val pos = e.get("start").get.toString
        (label, word, pos)
      }
  }

  def titleConceptFilter(label: String, word: String, products: Set[String],
       brands: Set[String], needAttrs: Set[String]): Boolean = {
    if (!needAttrs.contains(label)) return false
    if (!word.matches("[\u4e00-\u9fff\\-\\.,:a-zA-Z0-9]+")) return false
    products.foreach { x =>
      if (word.contains(x)) return false
    }
    brands.foreach { x =>
      if (word.contains(x)) return false
    }
    if (((" "+word+" ").split ("[\u4e00-\u9fff]").size -1) > 6) return false
    true
  }

  def makeTitleConceptJson(atts: List[(String, String, String)]): String = { 
    // Map[String,List[(String, String)]
    val concepts = atts.map (x => (x._1 +"_"+x._2, x._3)).groupBy(_._1)
    var json: JObject = JObject()
    concepts.keys.foreach{ i =>
        json = json ~ (i, concepts(i).map(_._2).mkString(","))
      }   
    compact(render(json))
  }

  def makeExtConceptJson(atts: Map[String, String]): String = {
    if (atts.size > 0) {
      compact(render(atts.map(x => ("att_"+x._1 +"_"+x._2, ""))))
    } else {
      "{}"
    }
  }

  def extractExtConcept(attMap: Map[String, String], products: Set[String],
      brands: Set[String], titleWords: String): Map[String, String] = {
    val filterWords = Set("品牌", "型号","颜色","热词","机型","功能",
      "材质","样式","风格","人群","适用","场景","季节","地区","味道")
    //属性名过滤
    var attMap_ = attMap.filterKeys{ p =>
        var result = true
        //关键词过滤
        filterWords.foreach { w =>
          if (p.contains(w)) result = false
        }
        //长度过滤
        if (result) {
          if (p.length > 6) result = false
        }
        //特殊符号过滤
        if (result) {
          if (!p.matches("[\u4e00-\u9fff\\-\\.,:a-zA-Z0-9]+")) result = false
        }
        result
      }
    //属性值过滤
    attMap_ = attMap_.filter { case(k, v) =>
      var result = true
      //长度过滤
      if (v.length <= 1) result = false
      //中文
      if (result) {
        if (!v.matches("[\\u4e00-\\u9fff]+")) result = false
      }
      //颜色词
      if (result) {
        val color = "金银蓝黑黄橙粉红绿灰白紫青棕色棕".toSet
        if (color.contains(v.charAt(v.length -1))) result = false
      }
      //产品词
      if (result) {
         products.foreach { p =>
          if (v.contains(p)) result = false
        }
      }
      //品牌词
      if (result) {
        if (brands.filter(t => v.contains(t) || t.contains(v)).size > 0) result = false
      }
      //与title所抽的value冲突
      if (result) {
        if (titleWords !="" && (titleWords.contains(v)
          || v.contains(titleWords))) result = false
      }
      result
    }
    attMap_
  }
}
