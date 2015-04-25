package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Top 10 skills in a city
 */
object ReqCitySkillsTopTen {

  def main(args: Array[String]) {
    val city = args(0)

    println("-> Top 10 skills in " + city)

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqCitySkillsTopTen")
    val sqlContext = new SQLContext(sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonFile("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/fullflatdistinct/part-*")
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
