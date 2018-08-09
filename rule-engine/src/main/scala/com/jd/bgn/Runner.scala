package com.jd.bgn.rule_engine

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.commons.cli.{Options, ParseException, PosixParser, CommandLine}
import com.jd.bgn.defenitions.Config

object Runner {
  private final val logger = LoggerFactory.getLogger(Runner.getClass)

  private final val CLI_OPT_DATE = "date"
  private final val CLI_OPT_TARGET_DB_TABLE = "taget_db_table"
  private final val CLI_SOURCE_ID = "source_id"
  private final val CLI_REPARTITION = "repartition"
  private final val CLI_USE_LOCAL_ATTR_SET = "use_local_attr_set"
  private final val CLI_RULE_FILE = "rule_file"

  def main(args: Array[String]): Unit = {
  	val parsed_cli = {
      try {
        val options = new Options()
            .addOption(null, CLI_OPT_DATE, true, "task date")
            .addOption(null, CLI_OPT_TARGET_DB_TABLE, true, "db.table to save result")
            .addOption(null, CLI_SOURCE_ID, true, "algorithm id")
            .addOption(null, CLI_REPARTITION, true, "repartition number")
            .addOption(null, CLI_USE_LOCAL_ATTR_SET, true, "whether use local attr set in file")
            .addOption(null, CLI_RULE_FILE, true, "path of rule description file")
        new PosixParser().parse(options, args)
      }
      catch {
        case e: ParseException =>
          logger.error("Failed to parse command line options", e)
          throw new Error("Failed to parse command line options", e)
      }
    }

    for (option <- parsed_cli.getOptions)
      logger.info(s"Command line option: ${option.getArgName} => ${option.getValue}")

    // Step 2: Loads properties if applicable
    for (key <- System.getProperties.stringPropertyNames.asScala)
      logger.info(s"System property: $key => ${System.getProperty(key)}")
    // Step 3: Extracts essential settings from command line options
    
    val date = getRequiredOptionValue(parsed_cli, CLI_OPT_DATE)
    val target_db_table = getRequiredOptionValue(parsed_cli, CLI_OPT_TARGET_DB_TABLE)
    val source_id = getRequiredOptionValue(parsed_cli, CLI_SOURCE_ID)
    val repartition = getRequiredOptionValue(parsed_cli, CLI_REPARTITION).toInt
    val use_local_attr_set = if (getRequiredOptionValue(parsed_cli, CLI_USE_LOCAL_ATTR_SET) == "true") true else false
    val rule_file = getRequiredOptionValue(parsed_cli, CLI_RULE_FILE)

    val rules = new RulesGenerator(rule_file).generateRules()
    val attr_group_map = new RulesGenerator(rule_file).generateAttrGroupMap()
    val config = Config(date, target_db_table, source_id, repartition, use_local_attr_set)
    new RuleEngine(config, attr_group_map, rules._1, rules._2, rules._3, rules._4).run()
  }
  private def getRequiredOptionValue(parsed_cli: CommandLine, name: String): String = {
    val value = {
      if (parsed_cli.hasOption(name)) {
      	parsed_cli.getOptionValue(name)
      }
      else {
      	throw new IllegalArgumentException(s"Missing mandatory option <$name>")
      }
    }
    require(value.nonEmpty)
    value
  }
}
