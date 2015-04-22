package com.octo

import java.text.SimpleDateFormat

import com.octo.types.Person
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

import scala.xml.XML



/**
 * Exercice 1 : Compte le nombre d'entreprises à Sydney qui font du Java
 */
object Exercise1 {
  // Not serializable so we make it transient and lazy
  //implicit val formats = DefaultFormats // Brings in default date formats etc.
  @transient lazy implicit private val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  }

  def main(args: Array[String]) {
    println("Starting exercise 1... ")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "Exercise1")

    val files = sc.wholeTextFiles("/Users/alex/Development/spark/LinkedIn-Spark/src/main/resources/extract/*.txt")

    val persons: RDD[Person] = files.flatMap { file =>
      val xml = XML.loadString(file._2)
      xml.map(Person.parseXml(_))
    }

    persons.foreach(p => println(p))

    persons.foreach(p => println(write(p)))

    val count = persons.count()
    println("Number of persons: " + count)
  }
}
