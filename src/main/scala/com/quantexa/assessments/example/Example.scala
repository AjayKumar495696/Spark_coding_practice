package com.quantexa.assessments.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Example extends App {


  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  case class CustomerData(
                           customerId: String,
                           forename: String,
                           surname: String
                         )
  case class FullName(
                       firstName: String,
                       surname: String
                     )

  case class CustomerModel(
                            customerId: String,
                            forename: String,
                            surname: String,
                            fullname: FullName
                          )

  val customerData = spark.read.option("header","true").csv("src/main/resources/customer_data.csv").as[CustomerData]

  val customerModel = customerData
    .map(
      customer =>
        CustomerModel(
          customerId = customer.customerId,
          forename = customer.forename,
          surname = customer.surname,
          fullname = FullName(
            firstName = customer.forename,
            surname = customer.surname))
    )

  customerModel.show(truncate = false)

  customerModel.write.mode("overwrite").parquet("src/main/resources/customerModel.parquet")
}