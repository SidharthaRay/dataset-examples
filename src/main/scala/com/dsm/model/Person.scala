package com.dsm.model

// Person domain object
case class Person(name:String, age:Long) {
  override def toString: String = s"Person($name,$age)"
}