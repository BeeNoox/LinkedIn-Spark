package com.octo

import java.text.SimpleDateFormat

import com.octo.types.{CompanySkillsView, Person}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.json4s._

import org.json4s.jackson.Serialization.write

import scala.xml.XML



/**
 * Exercise 1 : Count the number of companies in Sydney working with Java
 */
object Exercise1 {

  // DefaultFormats isn't serializable so we make it transient and lazy
  // DefaultFormats brings in default date formats for JSON serialization
  @transient lazy implicit private val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  }

  def main(args: Array[String]) {
    println("Starting exercise 1... ")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "Exercise1")
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    // Read XML files
    val files = sc.wholeTextFiles("/Users/alex/Development/spark-vagrant/LinkedIn-Spark/src/main/resources/extract/*.txt")

    // Convert each XML file to a Person
    val persons: RDD[Person] = files.flatMap { file =>
      val xml = XML.loadString(file._2)
      xml.map(Person.parseXml(_))
    }

    // RDD of serialized JSON
    val json = persons.map(write(_))

    // Registering JSON persons in hive context
    val personsTable = hiveContext.jsonRDD(json)
    personsTable.registerTempTable("persons")
    hiveContext.cacheTable("persons")

    // Creating a view
    val viewSql =
      """
        |SELECT c.universalname, c.name, location, c.isCurrent, s.name
        |FROM persons
        |LATERAL VIEW explode(skills) skillsTable AS s
        |LATERAL VIEW explode(companies) companiesTable AS c
      """.stripMargin
    val view = hiveContext.sql(viewSql)
      .map(row => CompanySkillsView(row.getString(0), row.getString(1), row.getString(2), row.getBoolean(3), row.getString(4)))
      .map(write(_))

    // Insert flattened data into a Spark SQL table
    val schemaRDD = sqlContext.jsonRDD(view)
    schemaRDD.registerTempTable("view")
    schemaRDD.printSchema()

    // Number of companies working with Java in Sydney
    val sqlReq =
    """
      |SELECT location, count(name) as
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
