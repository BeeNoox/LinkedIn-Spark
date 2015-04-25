package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Count the number of companies in Sydney working with Java
 */
object ReqCitySkillCompanyCount {

  def main(args: Array[String]) {
    println("-> Count the number of companies in Sydney working with Java")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ReqCitySkillCompanyCount")
    val sqlContext = new SQLContext(sc)

    // RDD of serialized JSON
    val json = LinkedInHelper.toJSON("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/extract/*.txt", sc)

    // Flatten the serialized Person JSON to a view
    val view = LinkedInHelper.toFlatView(json, sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonRDD(view)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Number of companies working with Java in Sydney
    val sqlReq =
      """
        |SELECT location, count(name)
        |FROM view
        |WHERE isCurrent = true
        |AND skill = 'Java'
        |AND location LIKE '%Sydney%'
        |GROUP BY location
      """.stripMargin
    val result = sqlContext.sql(sqlReq).collect()

    result.foreach(println(_))
  }
}
