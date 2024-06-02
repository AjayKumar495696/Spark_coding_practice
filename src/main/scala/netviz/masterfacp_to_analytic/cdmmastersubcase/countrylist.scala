/*
package netviz.masterfacp_to_analytic.cdmmastersubcase
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.{Duration, LocalDateTime}
import com.netviz.fciait.masterfacp_to_analytic.AbstractTableList
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import java.util.Calendar

object countrylist extends AbstractTableList with Logging {

  //Spark Configuration
  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

  /**
   * @author ajaysomarthy@gmail.com
   * @param environment Specify environment like prod, test, dev etc
   * @param sourcedb Database where is table to copy from
   * @param sourcetable table to copy from
   * @param targetdb Database where final table should exist
   * @param targettable Table name which should be product of etl
   */
  def initialize(environment: String,
                 sourcedb: String,
                 sourcetable: String,
                 targetdb: String,
                 targettable: String,
                 deduplicationFlag: String)={

    val applicationStartTime = LocalDateTime.now()
    logger.info("countrylist ETL: processes started : "+ applicationStartTime)
    logger.info("countrylist ETL: Reading data from hive table : "+sourcedb+"."+sourcetable)

    //Define Control Table Across environments
    var controltable = environment + "_control"
    if(environment.toUpperCase == "PROD"){
      controltable = "control"
    }

    //Create control table if not exist
    createControltable(targetdb,controltable,spark)

    //Drop data table anyway
    dropTableByName(targetdb,targettable,spark)

    //Create Data Frames
    val country = getTableAsDataFrameWithCurrentRecords(sourcedb, sourcetable, spark)

    country.createOrReplaceTempView("country_view")

    val country_list = spark.sql("select country_view.country_code,"+
    "country_view.country_name,"+
    "country_view.iso_code,"+
    "country_view.tax_haven_flag"+
    "cast(country_view.version_id as int) as version_id,"+
    "cast(country_view.nrd-aml_risk_score as double) as aml_risk_score,"+
    "'' as eu_flag,"+
    "'' as eea_flag,"+
    "contry_view.nrd_juridiction as juridiction,"+
    "cast(country_view.cpi as int) as cpi,"+
    "FROM country_view ")

    //deduplication
    val deduplicatedCountryList = checkDeduplicationFlagAndPerformLogic(country_list,
      Seq("country_code","country_name"), deduplicationFlag, targetdb, targettable, spark)

    //save dataframe as table
    storeDataFrameAsParquet(deduplicatedCountryList:Any,targetdb,targettable)

    //Get system date and time
    val date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val c = Calendar.getInstance()
    val cdcColumnMax = date.format(c.getTime())

    //Obtain stats from created table
    logger.info("countrylist ETL: Calculating statistical data")
    val targetTableRowCount=getNumberOfRowsInTable(targetdb,targettable,spark)
    logger.info("countrylist ETL: Target Table Raw Count : " + targetTableRowCount)
    logger.info("countrylist ETL: cdcColumnMax value : " + cdcColumnMax)

    //Insert those stats into control table
    insertStaticDataIntoControlTable(sourcedb,sourcetable,targettable,controltable,cdcColumnMax,targetTableRowCount,spark)

    val applicationEndTime = LocalDateTime.now()
    logger.info("countrylist ETL: process completed : " + applicationEndTime)

    val totalExecutionTime = Duration.between(applicationStartTime, ApplicationEndTime).toMillis
    logger.info("countrylist ETL: Total Execution Time in milliseconds : "+ totalExecutionTime)

  }

}
*/
