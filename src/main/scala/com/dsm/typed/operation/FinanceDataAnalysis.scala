package com.dsm.typed.operation

import com.dsm.model.Transaction
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object FinanceDataAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataset Demo")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    case class Account(number: String, firstName: String, lastName: String)

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    implicit val txnEncoder = Encoders.product[Transaction]
    val financeDs = sparkSession.read
        .parquet(finFilePath)
        .withColumn("date", to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp")))
        .as[Transaction]

    financeDs.printSchema()
    financeDs.show(3, false)

    val accNumPrev4WindowSpec = Window.partitionBy($"AccountNumber")
      .orderBy($"Date")
      .rowsBetween(-4, 0)

    println("typed transformation,")
    financeDs
        .select(
          $"AccountNumber".as[String],
          $"Amount".as[Double],
          $"date".as[java.sql.Date](Encoders.DATE),
          $"Description".as[String]
        )
        .show(5, false)

    println("untyped transformation,")
    financeDs
      .withColumn("RollingAvg", avg($"Amount").over(accNumPrev4WindowSpec).as[Double])
      .show(false)

    financeDs
        .groupByKey(fin => fin.AccountNumber)
        .agg(
          typed.avg[Transaction](_.Amount).as("AverageTransaction").as[Double],
          typed.sum[Transaction](_.Amount).as("TransactionSum").as[Double],
          typed.count[Transaction](_.Amount).as("TransactionCount").as[Double],
          max($"Amount").as("TransactionSum").as[Double]
        )
        .show()

    sparkSession.close()
  }
}
