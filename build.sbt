name := "LinkedIn-Spark"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" %% "spark-sql" % "1.3.1",
  "org.apache.spark" %% "spark-hive" % "1.3.1",
  "org.json4s" %% "json4s-jackson" % "3.2.11")
