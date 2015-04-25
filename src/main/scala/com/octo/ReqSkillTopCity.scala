package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * City working the most with a skill
 */
object ReqSkillTopCity {

  def main(args: Array[String]) {
    val skill = args(0)

    println("-> City working the most with " + skill)

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqSkillTopCity")
    val sqlContext = new SQLContext(sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonFile("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/fullflat/part-*")
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Count skills in a city
    val sqlReq =
      """
        |SELECT location, count(location) AS c
        |FROM view
        |WHERE isCurrent = true
        |AND skill = '""" + skill + """'
        |GROUP BY location
        |ORDER BY c DESC
      """
    val result = sqlContext.sql(sqlReq.stripMargin).collect()

    result.foreach(println(_))
  }
}
