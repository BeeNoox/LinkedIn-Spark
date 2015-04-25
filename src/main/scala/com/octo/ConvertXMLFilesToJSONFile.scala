package com.octo

import java.text.SimpleDateFormat

import com.octo.types.Person
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import scala.xml.XML

/**
 * Convert LinkedIn export XML files into JSON file(s)
 * arg1: input file or folder
 * arg2: output folder
 */
object ConvertXMLFilesToJSONFile {
  // DefaultFormats isn't serializable so we make it transient and lazy
  // DefaultFormats brings in default date formats etc.
  @transient lazy implicit private val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  }

  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)

    println("Converting LinkedIn XML file(s) to a JSON file")
    println("- input files arg: " + input)
    println("- output dir arg: " + output)

    // Spark Context setup
    val sc = new SparkContext("local[4]", "ConvertXMLFilesToJSONFile")

    // Read XML files
    val files = sc.wholeTextFiles(input)

    // Convert each XML file to a Person
    val persons: RDD[Person] = files.flatMap { file =>
      val xml = XML.loadString(file._2)
      xml.map(Person.parseXml(_))
    }

    // RDD of JSON strings
    val json = persons.map(write(_))

    // Write a JSON formatted file(s)
    // number of files depending of computation distribution
    json.saveAsTextFile(output)

    val count = persons.count()
    println("Number of persons: " + count)
  }
}
