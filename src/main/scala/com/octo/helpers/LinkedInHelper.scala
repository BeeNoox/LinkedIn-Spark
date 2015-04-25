package com.octo.helpers

import java.text.SimpleDateFormat

import com.octo.types.{CompanySkillsView, Person}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import scala.xml.XML

/**
 * Helper to convert LinkedIn data
 */
object LinkedInHelper {

  // DefaultFormats isn't serializable so we make it transient and lazy
  // DefaultFormats brings in default date formats for JSON serialization
  @transient lazy implicit private val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  }

  /**
   * Convert XML files to a JSON RDD
   *
   * @param input input files
   * @param sc current SparkContext
   * @return JSON RDD
   */
  def toJSON(input: String, sc: SparkContext): RDD[String] = {

    // Read XML files
    val files = sc.wholeTextFiles(input)

    // Convert each XML file to a Person
    val persons: RDD[Person] = files.flatMap { file =>
      val xml = XML.loadString(file._2)
      xml.map(Person.parseXml)
    }

    // RDD of JSON strings
    persons.map(write(_))
  }

  /**
   * Transform a RDD of JSON serialized Person to a flat view
   *
   * @param jsonRDD RDD of serialized JSON Persons
   * @param sc current SparkContext
   * @return RDD of Person JSON
   */
  def toFlatView(jsonRDD: RDD[String], sc: SparkContext): RDD[String] = {
    val hiveContext = new HiveContext(sc)

    // Registering JSON persons in hive context
    val personsTable = hiveContext.jsonRDD(jsonRDD)
    personsTable.registerTempTable("persons")
    hiveContext.cacheTable("persons")

    // Creating a view
    val viewSql =
    """
      |SELECT DISTINCT c.universalname, c.name, location, c.isCurrent, s.name
      |FROM persons
      |LATERAL VIEW explode(skills) skillsTable AS s
      |LATERAL VIEW explode(companies) companiesTable AS c
    """.stripMargin
    val view = hiveContext.sql(viewSql)
      .map(row => CompanySkillsView(row.getString(0), row.getString(1), row.getString(2), row.getBoolean(3), row.getString(4)))
      .map(write(_))

    view
  }
}
