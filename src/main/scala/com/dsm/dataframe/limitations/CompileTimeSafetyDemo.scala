package com.dsm.dataframe.limitations

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object CompileTimeSafetyDemo {

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

    // Applying filter on dataframe
    peopleDf.filter("ages > 20").show()

    spark.stop()
  }
}
