package com.dsm.dataframe.limitations

import com.dsm.model.Person
import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

object OperationOnDomainObject {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Compile-time safety - Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val persons = Seq(
      Person("Sidharth", 30),
      Person("Vinay", 26),
      Person("Piyush", 25)
    )

    // Create RDD[Person]
    val personRdd = spark.sparkContext.makeRDD(persons)

    println("Iterating the RDD[Person],")
    personRdd.collect().foreach(println)

    println("\nArray[Person],")
    println(personRdd.collect())

    // Create Dataframe from an RDD[Person]
    println("\nDataframe,")
    val personDf = spark.createDataFrame(personRdd)
    personDf.show()

    // We get back RDD[Row] not the original RDD[Person]
    println("\nShould be Array[Person], but it's not!")
    println(personDf.rdd.collect())

    spark.stop()
  }
}
