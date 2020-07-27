package com.dsm.typed.operation

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
 * Created by Sidharth on 27/01/17.
 */
object DistributedWordCount {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().master("local[*]").appName("RDD Comparision").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val wordFilePath = s"s3n://${s3Config.getString("s3_bucket")}/words.txt"
    val linesRdd = sparkSession.sparkContext.textFile(wordFilePath)
    val wordsRdd = linesRdd
      .flatMap(line => line.split(" "))
      .filter(word => word != "")
    val wordCounts = wordsRdd.groupBy(word => word.toLowerCase).map(wordCount => (wordCount._1, wordCount._2.size))
    println("RDD contents")
    wordCounts.foreach(println)

    import sparkSession.implicits._
    val linesDS = sparkSession.read.text(wordFilePath).as[String]
    val wordsDS = linesDS
      .flatMap(line => line.split(" "))
      .filter(word => word != "")
    val wordCountDS = wordsDS.groupByKey[String]((word:String) => word.toLowerCase).count()
    println("Dataset contents")
    wordCountDS.show()

    sparkSession.stop()
  }

}
