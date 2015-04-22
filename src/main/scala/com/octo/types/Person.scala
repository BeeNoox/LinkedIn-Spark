package com.octo.types

/**
 * Define a linkedin person class
 */
case class Person(firstname: String,
                  lastname: String,
                  location: String,
                  companies: Seq[Company],
                  skills:Seq[Skill])

/**
 * Define functions on a linkedin person class
 */
object Person {
  def parseXml(n: scala.xml.Node):
    Person = new Person(
      getText(n, "first-name"),
      getText(n, "last-name"),
      getLocation(n),
      Company.getCompanies(n),
      Skill.getSkills(n)
    )

  def getText(node: scala.xml.Node, tagName: String):
    String = (node \ tagName).text.trim()

  def getLocation(node: scala.xml.Node):
    String = (node \ "location" \ "name").text.trim()
}
