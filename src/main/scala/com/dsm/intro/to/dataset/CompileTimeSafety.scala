package com.dsm.intro.to.dataset

import com.dsm.model.Person
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object CompileTimeSafety {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Compile-time safety - Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    // Create dataframe from json file
    val peopleJsonPath = s"s3n://${s3Config.getString("s3_bucket")}/people.json"
    val peopleDf = spark.read.json(peopleJsonPath)

    peopleDf.printSchema()

    // Get a dataset out of dataframe
    import spark.implicits._
    val peopleDs = peopleDf.as[Person]
    peopleDs.filter(p => p.age > 26).show()

    // The below will give you compilation error
    // peopleDs.filter(p => p.salary > 1000).show()

    spark.stop()
  }
}
