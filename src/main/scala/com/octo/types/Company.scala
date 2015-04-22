package com.octo.types

/**
 * Define a linkedin skill class
 */
case class Company(name: String,
                   universalname: String,
                   isCurrent: Boolean)

/**
 * Define functions on a linkedin person class
 */
object Company {
  def getCompanies(node: scala.xml.Node):
    Seq[Company] = (node \ "positions" \ "position").map { position =>
      Company((position \ "company" \ "name").text,
        (position \ "company" \ "universal-name").text,
        (position \ "is-current").text.toBoolean )
    }

  def toString(c: Company):
    String = "Company: (name:" + c.name + ", universalName:" + c.universalname + ", isCurrent:" + c.isCurrent + ")"
}
