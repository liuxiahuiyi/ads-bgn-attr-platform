package com.jd.bgn

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat

private object StrArrayParam {
  def unapply(str: String): Option[Array[String]] = {
    Some(str.split(",").map(_.trim).toArray)
  }
}

class CommonArgs(args: Array[String]) {
  var cal = Calendar.getInstance
  cal.add(Calendar.DATE, -1)
  var date: Date = cal.getTime
  var projectPath: String = "yangxi/sandbox"
  var hivePath: String = "yangxi/hivebox"
  var partitionCategory: String = "test"
  parse(args.toList)

  private def parse(args: List[String]): Unit = args match {
    case ("--date") :: value :: tail =>
      date = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--project-path") :: value :: tail =>
      projectPath = value
      parse(tail)
    
    case ("--hive-path") :: value :: tail =>
      hivePath = value
      parse(tail)

    case ("--partition-category") :: value :: tail =>
      partitionCategory = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil =>

    case _ =>
      printUsageAndExit(1)
  }

  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Worker [options]\n" +
        "\n" +
        "Options:\n" +
        "  --date DATE process date\n" +
        "  --project-path\n" +
        "  --hive-path\n" +
        "  --partition-category\n" +
        "\n")
    System.exit(exitCode)
  }
}
