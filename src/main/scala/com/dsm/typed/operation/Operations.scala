package com.dsm.typed.operation

import org.apache.spark.sql.SparkSession

/**
 * Created by Sidharth on 27/01/17.
 */
object Operations {

  val peopleFilePath = "src/main/resources/people.csv"

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().master("local[*]").appName("RDD Comparision").getOrCreate()

    val linesRdd = sparkSession.sparkContext.textFile(peopleFilePath)
    val wordsRdd = linesRdd
      .filter(line => line != "")
      .map(line => line.split(","))
      .map(rec => (rec(0), rec(1)))
    println("RDD contents")
    wordsRdd.foreach(println)

    import sparkSession.implicits._
    val linesDS = sparkSession.read.text(peopleFilePath).as[String]
    val wordsDS = linesDS
      .filter(line => line != "")
      .map(line => line.split(","))
      .map(rec => (rec(0), rec(1)))
    println("Dataset contents")
    wordsDS.show()

    sparkSession.stop()
  }

}
