package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * City working the most with Java
 */
object ReqSkillTopCity {

  def main(args: Array[String]) {
    println("-> City working the most with Java")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqSkillTopCity")
    val sqlContext = new SQLContext(sc)

    // RDD of serialized JSON
    val json = LinkedInHelper.toJSON("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/extract/*.txt", sc)

    // Flatten the serialized Person JSON to a view
    val view = LinkedInHelper.toFlatView(json, sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonRDD(view)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Count skills in a city
    val sqlReq =
      """
        |SELECT location, count(location) AS c
        |FROM view
        |WHERE isCurrent = true
        |AND skill = 'Java'
        |GROUP BY location
        |ORDER BY c DESC
      """.stripMargin
    val result = sqlContext.sql(sqlReq).collect()

    result.foreach(println(_))
  }
}
