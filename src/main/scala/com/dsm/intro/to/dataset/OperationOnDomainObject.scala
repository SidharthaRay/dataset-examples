package com.dsm.intro.to.dataset

import com.dsm.model.Person
import org.apache.spark.sql.SparkSession

object OperationOnDomainObject {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Comparision with Dataframe")
      .getOrCreate()

    val persons = Seq(
      Person("Sidharth", 30),
      Person("Vinay", 26),
      Person("Piyush", 25)
    )

    // Create RDD of Person
    val personRdd = spark.sparkContext.makeRDD(persons)
    personRdd.collect().foreach(println)
    println(personRdd.collect())

    // Create Dataset from an RDD[Person]
    import spark.implicits._
    val personDs = spark.createDataset(personRdd)
    personDs.show()

    // We get back RDD[Person]
    println(personDs.rdd.collect())

    spark.stop()
  }
}