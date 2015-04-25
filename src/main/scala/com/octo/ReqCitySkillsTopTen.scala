package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Top 10 skills in a city
 */
object ReqCitySkillsTopTen {

  def main(args: Array[String]) {
    println("-> Top 10 skills in a city")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqCitySkillsTopTen")
    val sqlContext = new SQLContext(sc)

    // RDD of serialized JSON
    val json = LinkedInHelper.toJSON("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/extract/*.txt", sc)

    // Flatten the serialized Person JSON to a view
    val view = LinkedInHelper.toFlatView(json, sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonRDD(view)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Skill count in a city
    val sqlReq =
      """
        |SELECT skill, count(skill) AS c
        |FROM view
        |WHERE isCurrent = true
        |AND location LIKE '%Sydney%'
        |GROUP BY skill
        |ORDER BY c DESC
      """.stripMargin
    val result = sqlContext.sql(sqlReq).collect()

    result.foreach(println(_))
  }
}
