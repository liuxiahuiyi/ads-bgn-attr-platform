package com.jd.bgn.rule_engine

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import java.util.Properties
import java.io.{InputStreamReader, FileInputStream}
import com.jd.bgn.rules.{Matching, Range, Common}

class RulesGenerator(path: String) extends Serializable {
  private final val logger = LoggerFactory.getLogger(RulesGenerator.getClass)
  private val defaults = Map("columns" -> "sku_name",
                             "category.split" -> "item_first_cate_cd",
                             "attr_value.length_mode" -> "char",
                             "attr_value.omit" -> "",
                             "rule" -> "",
                             "rule.matching.context.left.regex" -> "",
                             "rule.matching.context.right.regex" -> "",
                             "rule.matching.tolerance" -> "0",
                             "rule.matching.length_difference_tolerance" -> "1",
                            )
  private def readProperties(): Properties = {
    try {
      val properties = new Properties()
      properties.load(new InputStreamReader(new FileInputStream(path), "UTF-8"))
      properties
    }
    catch {
      case e =>
        logger.error(s"Failed to read properties file ${path}", e)
        throw new Error(s"Failed to read properties file ${path}", e)
    }
  }
  def generateAttrGroupMap(): Map[String, Map[String, String]] = {
    val properties = readProperties()
    val keys = properties.stringPropertyNames()
                         .filter(_.endsWith("com_attr_cds"))
    var attr_group_map = Map[String, Map[String, String]]()
    keys.foreach((key) => {
      val item_first_cate_cd = key.split(".")(0)
      val cate_attr_group_map = properties.getProperty(key)
                                          .split(",")
                                          .flatMap((e) => e.split("+").filter(_ != "").map((_, e)))
                                          .toMap
      attr_group_map = attr_group_map ++ Map(item_first_cate_cd -> cate_attr_group_map)
    })
    attr_group_map
  }
  def generateRules(): (Map[String, Map[String, Common]], Map[String, Map[String, Matching]], Map[String, Map[String, Range]]) = {
    val properties = readProperties()
    val keys = properties.stringPropertyNames()
    var common_rules = Map[String, Map[String, Common]]()
    var matching_rules = Map[String, Map[String, Matching]]()
    var range_rules = Map[String, Map[String, Range]]()
    val item_first_cate_cds = properties.getProperty("item_first_cate_cds").split(",").filter(_ != "")
    item_first_cate_cds.foreach((item_first_cate_cd) => {
      common_rules = common_rules ++ Map(item_first_cate_cd -> Map[String, Common]())
      matching_rules = matching_rules ++ Map(item_first_cate_cd -> Map[String, Matching]())
      range_rules = range_rules ++ Map(item_first_cate_cd -> Map[String, Range]())
      val com_attr_cd_groups = properties.getProperty(s"${item_first_cate_cd}.com_attr_cds").split(",").filter(_ != "")
      com_attr_cd_groups.foreach((com_attr_cd_group) => {
        val cate_attr_group_id = if (properties.getProperty(s"${item_first_cate_cd}.${com_attr_cd_group}.like") != null) {
          properties.getProperty(s"${item_first_cate_cd}.${com_attr_cd_group}.like")
        } else {
          s"${item_first_cate_cd}.${com_attr_cd_group}"
        }

        val columns = properties.getProperty(s"${cate_attr_group_id}.columns") match {
          case null => defaults("columns")
          case _ => properties.getProperty(s"${cate_attr_group_id}.columns")
        }.split(",").filter(_ != null)
        val category_split = properties.getProperty(s"${cate_attr_group_id}.category.split") match {
          case null => defaults("category.split")
          case _ => properties.getProperty(s"${cate_attr_group_id}.category.split")
        }
        val attr_value_length_mode = properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode") match {
          case null => defaults("attr_value.length_mode")
          case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode")
        }
        val attr_value_omit = properties.getProperty(s"${cate_attr_group_id}.attr_value.omit") match {
          case null => defaults("attr_value.omit")
          case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.omit")
        }.split(",").filter(_ != null)
        val rule = properties.getProperty(s"${cate_attr_group_id}.rule") match {
          case null => defaults("rule")
          case _ => properties.getProperty(s"${cate_attr_group_id}.rule")
        }
        common_rules(item_first_cate_cd) = common_rules(item_first_cate_cd)
          ++ Map(com_attr_cd_group -> new Common(columns, category_split, attr_value_length_mode, attr_value_omit, rule))

        rule match {
          case "matching" => {
            val length_mode = properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode") match {
              case null => defaults("attr_value.length_mode")
              case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode")
            }
            val context_left = properties.getProperty(s"${cate_attr_group_id}.rule.matching.context.left.regex") match {
              case null => defaults("rule.matching.context.left.regex")
              case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.context.left.regex")
            }
            val context_right = properties.getProperty(s"${cate_attr_group_id}.rule.matching.context.right.regex") match {
              case null => defaults("rule.matching.context.right.regex")
              case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.context.right.regex")
            }
            val tolerance = properties.getProperty(s"${cate_attr_group_id}.rule.matching.tolerance") match {
              case null => defaults("rule.matching.tolerance")
              case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.tolerance")
            }.toInt
            val length_difference_tolerance = properties.getProperty(s"${cate_attr_group_id}.rule.matching.length_difference_tolerance") match {
              case null => defaults("rule.matching.length_difference_tolerance")
              case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.length_difference_tolerance")
            }.toInt
            val custom = keys.filter(_.startsWith(s"${cate_attr_group_id}.rule.matching.custom.regex"))
                             .map((e) => (e.replaceFirst(s"${cate_attr_group_id}.rule.matching.custom.regex", ""), properties.getProperty(e)))
                             .toMap
            matching_rules(item_first_cate_cd) = matching_rules(item_first_cate_cd)
              ++ Map(com_attr_cd_group -> new Matching(length_mode, context_left, context_right, tolerance, length_difference_tolerance, custom))
          }
          case "range" => {
            range_rules(item_first_cate_cd) = range_rules(item_first_cate_cd)
              ++ Map(com_attr_cd_group -> new Range())
          }
          case _ => {
            logger.error(s"unknown rule name ${rule}")
            throw new Error(s"unknown rule name ${rule}")
          }
        }
      })
    })
    (common_rules, matching_rules, range_rules)
  }
}

