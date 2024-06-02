package com.quantexa.assessments.scoringModel

import com.quantexa.assessments.accounts.AccountAssessment.AccountData
import com.quantexa.assessments.customerAddresses.CustomerAddress.AddressData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/***
  * Part of the Quantexa solution is to flag high risk countries as a link to these countries may be an indication of
  * tax evasion.
  *
  * For this question you are required to populate the flag in the ScoringModel case class where the customer has an
  * address in the British Virgin Islands.
  *
  * This flag must be then used to return the number of customers in the dataset that have a link to a British Virgin
  * Islands address.
  */

object ScoringModel extends App {
  //Logger.getLogger("org").setLevel(Level.ERROR)

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)
  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[(String, String, Long)],
                               //Addresses for this customer
                               address : Seq[(String, String, Option[Int], Option[String], Option[String], Option[String])]
                             )

  case class ScoringModel(
                           customerId: String,
                           forename: String,
                           surname: String,
                           //Accounts for this customer
                           accounts: Seq[(String, String, Long)],
                           //Addresses for this customer
                           address : Seq[(String, String, Option[Int], Option[String], Option[String], Option[String])],
                           linkToBVI: Boolean
                         )


  //END GIVEN CODE

  import spark.implicits._

  val customerDocumentDS = spark.read.parquet("src/main/resources/customerDocument.parquet").as[CustomerDocument]

  def isAddressInBritishVirginIslands(address: (String, String, Option[Int], Option[String], Option[String], Option[String])): Boolean = {
    address._6.exists(_.equalsIgnoreCase("British Virgin Islands"))
  }

  val scoringModelDS = customerDocumentDS.map(customer => {
    val linkToBVI = customer.address.exists(isAddressInBritishVirginIslands)
    ScoringModel(
      customer.customerId,
      customer.forename,
      customer.surname,
      customer.accounts,
      customer.address,
      linkToBVI
    )
  })

  val scoringModelOutputDS = scoringModelDS.as[ScoringModel]
  scoringModelOutputDS.show(1000,false)
}
