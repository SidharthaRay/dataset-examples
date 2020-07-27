package com.dsm.dataframe.limitations

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object FuncProgApi {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Functional Prog API - Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val employeePath = s"s3n://${s3Config.getString("s3_bucket")}/employee"
    val employeeDf = spark.read.csv(employeePath).toDF("name", "date_of_birth")

    employeeDf.printSchema()

    // Step 2 - Register the calculateAge function as a UDF
    val calculateAgeUdf = udf[Int, String](calculateAge)

    // Step 3 - Use the UDF in withColumn/select function
    employeeDf.withColumn("age", calculateAgeUdf(employeeDf("date_of_birth"))).show()
    spark.stop()
  }

  // Step 1 - Declare Scala function - calculateAge
  def calculateAge(date_of_birth:String): Int = {
    val birthDay = Calendar.getInstance()
    birthDay.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(date_of_birth))
    val today = Calendar.getInstance()
    return today.get(Calendar.YEAR) - birthDay.get(Calendar.YEAR)
  }
}
