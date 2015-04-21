package com.octo.types

/**
 * Define a linkedin person class
 */
case class Person(firstname: String,
             lastname: String)

/**
 * Define functions on a linkedin person class
 */
object Person {
  def parseXml(n: scala.xml.Node):
    Person = new Person(
      getText(n, "first-name"),
      getText(n, "last-name")
    )

  def getText(node: scala.xml.Node, tagName: String):
    String = (node \ tagName).text.trim()

  def toString(p: Person):
    String = "(" + p.firstname + ", " + p.lastname + ")"
}