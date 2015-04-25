package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Top 10 skills in a city
 */
object ReqCitySkillsTopTen {

  def main(args: Array[String]) {
    val city = args(0)
    val files = args(1)

    println("-> Top 10 skills in " + city)
    println("(File(s) used:" + files + ")")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqCitySkillsTopTen")
    val sqlContext = new SQLContext(sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonFile(files)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Skill count in a city
    val sqlReq =
      """
        |SELECT skill, count(skill) AS c
        |FROM view
        |WHERE isCurrent = true
        |AND location LIKE '%""" + city + """%'
        |GROUP BY skill
        |ORDER BY c DESC
      """
    val result = sqlContext.sql(sqlReq.stripMargin).collect()

    result.foreach(println(_))
  }
}
