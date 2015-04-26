package com.octo

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Count the number of companies in a given city working with a given skill
 */
object ReqCitySkillCompanyCount {

  def main(args: Array[String]) {
    val city = args(0)
    val skill = args(1)
    val files = args(2)

    println("-> Count the number of companies in " + city + " working with skill " + skill)
    println("(File(s) used:" + files + ")")

    // Spark Context setup
    val sc = new SparkContext("local[8]", "ReqCitySkillCompanyCount")
    val sqlContext = new SQLContext(sc)

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonFile(files)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Number of companies working with Java in Sydney
    val sqlReq =
      """
        |SELECT location, count(name)
        |FROM view
        |WHERE isCurrent = true
        |AND skill = '""" + skill + """'
        |AND location LIKE '%""" + city + """%'
        |GROUP BY location
      """
    val result = sqlContext.sql(sqlReq.stripMargin).collect()

    result.foreach(println(_))
  }
}
