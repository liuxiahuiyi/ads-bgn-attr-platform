package com.jd.bgn.rule_engine

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import java.util.Properties
import java.io.{InputStreamReader, FileInputStream}
import com.jd.bgn.rules.{Matching, Range, Common, Age}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import org.apache.hadoop.fs.FSDataInputStream

class RulesGenerator(path: String) extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  private val defaults = Map("columns" -> "sku_name",
                             "remark_enable" -> "false",
                             "category.split" -> "item_second_cate_cd",
                             "revision.bulk" -> "invalid",
                             "supplement.min_fraction" -> "0.5",
                             "attr_value.length_mode" -> "char",
                             "attr_value.omit" -> "",
                             "attr_value.complement" -> "",
                             "attr_value.min_proprttion" -> "0",
                             "rule" -> "matching",
                             "rule.matching.context.left.regex" -> "",
                             "rule.matching.context.right.regex" -> "",
                             "rule.matching.tolerance" -> "0",
                             "rule.matching.length_difference_tolerance" -> "1")
  private def readProperties(): Properties = {
    try {
      val properties = new Properties()
      val input_stream_reader = if (path.startsWith("hdfs")) {
        val hdfs = FileSystem.get(URI.create(path), new Configuration)
        val fis = hdfs.open(new Path(path))
        new InputStreamReader(fis, "UTF-8")
      } else {
        new InputStreamReader(new FileInputStream(path), "UTF-8")
      }
      properties.load(input_stream_reader)
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
                         .asScala
                         .filter(_.endsWith("com_attr_cds"))
    var attr_group_map = Map[String, Map[String, String]]()
    keys.foreach((key) => {
      val item_first_cate_cd = key.split("\\.")(0)
      val cate_attr_group_map = properties.getProperty(key)
                                          .split(",")
                                          .flatMap((e) => e.split("\\+").filter(_ != "").map((_, e)))
                                          .toMap
      attr_group_map = attr_group_map ++ Map(item_first_cate_cd -> cate_attr_group_map)
    })
    attr_group_map
  }
  def generateRules(): (Map[(String, String), Common], Map[(String, String), Matching], Map[(String, String), Range], Map[(String, String), Age]) = {
    val properties = readProperties()
    val keys = properties.stringPropertyNames().asScala
    var common_rules = Map[(String, String), Common]()
    var matching_rules = Map[(String, String), Matching]()
    var range_rules = Map[(String, String), Range]()
    var age_rules = Map[(String, String), Age]()
    val item_first_cate_cds = properties.getProperty("item_first_cate_cds").split(",").filter(_ != "")
    item_first_cate_cds.foreach((item_first_cate_cd) => {
      val com_attr_cd_groups = properties.getProperty(s"${item_first_cate_cd}.com_attr_cds").split(",").filter(_ != "")
      com_attr_cd_groups.foreach((com_attr_cd_group) => {
        val cate_attr_group_id = if (properties.getProperty(s"${item_first_cate_cd}.${com_attr_cd_group}.like") != null) {
          properties.getProperty(s"${item_first_cate_cd}.${com_attr_cd_group}.like")
        } else {
          s"${item_first_cate_cd}.${com_attr_cd_group}"
        }

        val columns = {
          properties.getProperty(s"${cate_attr_group_id}.columns") match {
            case null => defaults("columns")
            case _ => properties.getProperty(s"${cate_attr_group_id}.columns")
          }
        }.split(",").filter(_ != "")
        val remark_enable = if ({
          properties.getProperty(s"${cate_attr_group_id}.remark_enable") match {
            case null => defaults("remark_enable")
            case _ => properties.getProperty(s"${cate_attr_group_id}.remark_enable")
          }
        } == "true") true else false
        val category_split = properties.getProperty(s"${cate_attr_group_id}.category.split") match {
          case null => defaults("category.split")
          case _ => properties.getProperty(s"${cate_attr_group_id}.category.split")
        }
        val revision_bulk = properties.getProperty(s"${cate_attr_group_id}.revision.bulk") match {
          case null => defaults("revision.bulk")
          case _ => properties.getProperty(s"${cate_attr_group_id}.revision.bulk")
        }
        val supplement_min_fraction = {
          properties.getProperty(s"${cate_attr_group_id}.supplement.min_fraction") match {
            case null => defaults("supplement.min_fraction")
            case _ => properties.getProperty(s"${cate_attr_group_id}.supplement.min_fraction")
          }
        }.toDouble
        val attr_value_length_mode = properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode") match {
          case null => defaults("attr_value.length_mode")
          case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.length_mode")
        }
        val attr_value_omit = {
          properties.getProperty(s"${cate_attr_group_id}.attr_value.omit") match {
            case null => defaults("attr_value.omit")
            case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.omit")
          }
        }.split(",").filter(_ != "")
        val attr_value_complement = {
          properties.getProperty(s"${cate_attr_group_id}.attr_value.complement") match {
            case null => defaults("attr_value.complement")
            case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.complement")
          }
        }.split(",")
         .filter(_ != "")
         .map((e) => {
           val block = e.split(":").filter(_ != "")
           if (block.length == 1) ("-1", block(0)) else (block(1), block(0))
         })
        val attr_value_min_proprttion = {
          properties.getProperty(s"${cate_attr_group_id}.attr_value.min_proprttion") match {
            case null => defaults("attr_value.min_proprttion")
            case _ => properties.getProperty(s"${cate_attr_group_id}.attr_value.min_proprttion")
          }
        }.toDouble
        val rule = properties.getProperty(s"${cate_attr_group_id}.rule") match {
          case null => defaults("rule")
          case _ => properties.getProperty(s"${cate_attr_group_id}.rule")
        }
        common_rules = common_rules ++ Map((item_first_cate_cd, com_attr_cd_group) -> new Common(
          columns, remark_enable, category_split, revision_bulk, supplement_min_fraction,
          attr_value_length_mode, attr_value_omit, attr_value_complement, attr_value_min_proprttion, rule))

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
            val tolerance = {
              properties.getProperty(s"${cate_attr_group_id}.rule.matching.tolerance") match {
                case null => defaults("rule.matching.tolerance")
                case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.tolerance")
              }
            }.toInt
            val length_difference_tolerance = {
              properties.getProperty(s"${cate_attr_group_id}.rule.matching.length_difference_tolerance") match {
                case null => defaults("rule.matching.length_difference_tolerance")
                case _ => properties.getProperty(s"${cate_attr_group_id}.rule.matching.length_difference_tolerance")
              }
            }.toInt
            val custom = keys.filter(_.startsWith(s"${cate_attr_group_id}.rule.matching.custom.regex"))
                             .map((e) => (e.replace(s"${cate_attr_group_id}.rule.matching.custom.regex.", ""), properties.getProperty(e)))
                             .toMap
            matching_rules = matching_rules ++ Map((item_first_cate_cd, com_attr_cd_group) -> new Matching(
              length_mode, context_left, context_right, tolerance, length_difference_tolerance, custom))
          }
          case "range" => {
            range_rules = range_rules ++ Map((item_first_cate_cd, com_attr_cd_group) -> new Range())
          }
          case "age" => {
            age_rules = age_rules ++ Map((item_first_cate_cd, com_attr_cd_group) -> new Age())
          }
          case _ => {
            logger.error(s"unknown rule name ${rule}")
            throw new Error(s"unknown rule name ${rule}")
          }
        }
      })
    })
    (common_rules, matching_rules, range_rules, age_rules)
  }
}

