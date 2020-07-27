package com.dsm.typed.operation

import com.dsm.model.{Employee, _}
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object JoinsDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val employeeDs = List(
      Emp(1, "Sidhartha", "Ray"),
      Emp(2, "Pratik", "Solanki"),
      Emp(3, "Ashok", "Pradhan"),
      Emp(4, "Rohit", "Bhangur"),
      Emp(5, "Kranti", "Meshram"),
      Emp(7, "Ravi", "Kiran")
    ).toDS()

    val empRoleDs = List(
      Role(1, "Architect"),
      Role(2, "Programmer"),
      Role(3, "Analyst"),
      Role(4, "Programmer"),
      Role(5, "Architect"),
      Role(6, "CEO")
    ).toDS()

//    employeeDs.joinWith(empRoleDs, $"id" === $"id").show(false)   // Ambiguous column name "id"
    employeeDs.joinWith(empRoleDs, employeeDs("id") === empRoleDs("id")).show(false)

    employeeDs.joinWith(empRoleDs, employeeDs("id") === empRoleDs("id"), "inner").show(false)    //"left_outer"/"left", "full_outer"/"full"/"outer"
    employeeDs.joinWith(empRoleDs, employeeDs("id") === empRoleDs("id"), "right_outer").show(false)    //"left"
//    employeeDs.joinWith(empRoleDs, employeeDs("id") === empRoleDs("id"), "left_anti").show(false)

//    employeeDs.joinWith(empRoleDs, employeeDs("id") === empRoleDs("id"), "cross").show(false)  // cross join

  }
}
