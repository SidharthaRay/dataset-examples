package com.dsm.dataframe.limitations

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.Row

object CompileTimeSafetySchemaOnRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Compile-time safety - Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val schema = StructType(List(StructField("test", BooleanType, true)))
    val rdd = spark.sparkContext.parallelize(List(Row(10.3), Row(true), Row("stuff")))

    spark.createDataFrame(rdd, schema)
      .show()

    spark.close()
  }
}
