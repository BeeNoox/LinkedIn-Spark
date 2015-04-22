package com.octo

import com.octo.types.Person
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.xml.XML

/**
 * Exercice 1 : Compte le nombre d'entreprises à Sydney qui font du Java
 */
object Exercise1 {
  def main(args: Array[String]) {
    println("Starting exercise 1... ")

    // Spark Context setup
    val sc = new SparkContext("local[4]", "Exercise1")

    val files = sc.wholeTextFiles("/Users/alex/Development/spark/octo-linkedin-java/src/main/resources/extract/*.txt")

    val persons: RDD[Person] = files.flatMap { file =>
      val xml = XML.loadString(file._2)
      xml.map(Person.parseXml(_))
    }

    persons.foreach(p => println(Person.toString(p)))

    val count = persons.count()
    println("Number of persons: " + count)
  }
}
