package com.dsm.model

case class Employee (name:String, dateOfBirth:String) {
  var age:Int = 0

  override def toString: String = s"Employee($name,$dateOfBirth,$age)"
}
