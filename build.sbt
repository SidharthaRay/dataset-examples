name := "dataset-examples"

version := "0.1"

scalaVersion := "2.11.8"

// Spark Core and Spark SQL dependencies
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"

// Reading data from s3 SBT dependency
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.4"

//configuration file dependency
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
