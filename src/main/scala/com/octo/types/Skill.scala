package com.octo.types

/**
* Define a linkedin skill class
*/
case class Skill(name: String)

/**
 * Define functions on a linkedin person class
 */
object Skill {
  def getSkills(node: scala.xml.Node):
    Seq[Skill] = (node \ "skills" \ "skill" \ "skill" \ "name").map { skill =>
      Skill(skill.text)
    }

  def toString(s: Skill):
    String = "Skill: (name: " + s.name + ")"
}
