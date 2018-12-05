package com.jd.bgn.item_img2txt

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import org.apache.commons.cli.{Options, ParseException, PosixParser, CommandLine}

object Runner {
  private final val logger = LoggerFactory.getLogger(Runner.getClass)

  private final val CLI_OPT_DATE = "date"
  private final val CLI_OPT_TARGET_DB_TABLE = "target_db_table"
  private final val CLI_LIBRARY_HDFS = "library_hdfs"
  private final val CLI_OCR_TRAINED_DATA_HDFS = "ocr_trained_data_hdfs"
  private final val CLI_ITEM_FIRST_CATE_CDS = "item_first_cate_cds"

  def main(args: Array[String]): Unit = {
  	val parsed_cli = {
      try {
        val options = new Options()
            .addOption(null, CLI_OPT_DATE, true, "task date")
            .addOption(null, CLI_OPT_TARGET_DB_TABLE, true, "db.table to save result")
            .addOption(null, CLI_LIBRARY_HDFS, true, "library hdfs")
            .addOption(null, CLI_OCR_TRAINED_DATA_HDFS, true, "ocr trained data hdfs")
            .addOption(null, CLI_ITEM_FIRST_CATE_CDS, true, "item first cate cds")
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
    val library_hdfs = getRequiredOptionValue(parsed_cli, CLI_LIBRARY_HDFS)
    val ocr_trained_data_hdfs = getRequiredOptionValue(parsed_cli, CLI_OCR_TRAINED_DATA_HDFS)
    val item_first_cate_cds = getRequiredOptionValue(parsed_cli, CLI_ITEM_FIRST_CATE_CDS)
    val config = Config(date, target_db_table, library_hdfs, ocr_trained_data_hdfs, item_first_cate_cds)
    new Transfomer(config).run()
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