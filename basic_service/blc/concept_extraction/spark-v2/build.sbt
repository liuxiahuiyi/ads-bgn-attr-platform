lazy val commonSettings = Seq(
  organization := "com.jd.bgn",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "bgn-blc",
  
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",

    libraryDependencies += "com.huaban" % "jieba-analysis" % "1.0.2",
    libraryDependencies += "org.json4s" %% "json4s-native" % "3.3.0",
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.2.1",

    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test",

    test in assembly := {}
  )

