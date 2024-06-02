/*
package netviz.masterfacp_to_analytic.cdmmastersubcase
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Calendar
import java.text.SimpleDateFormat
import java.time.{Duration, LocalDateTime}

object CdmMasterSubCase extends AbstractTableList with Logging {

  //Spark Configuration
  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

  /**
   * @author ajaysomarthy@gmail.com
   * @param environment Specify environment like prod,test,dev etc
   * @param sourcedb Database where is table to copy from
   * @param sourcetable table to copy general trade info from
   * @param targetdb Database where final table should exist
   * @param targettable Accuity CDM Master sub case table name for which should be product of etl
   */


  def initialize(environment: String,
                 sourcedb: String,
                 sourcetable: String,
                 targetdb: String,
                 targettable: String): Any = {

    val applicationStartTime = LocalDateTime.now()
    logger.info("Cdm master sub case ETL processes started : " + applicationStartTime)
    logger.info("Reading data from hive table " + sourcedb + "." + sourcetable)

    //Define Control Table Across environments
    var controltable = environment + "_control"
    if (environment.toUpperCase == "PROD"){
      controltable = "control"
    }

    //create control table if not exist
    createControlTable(targetdb, controltable, spark)

    //Processing Cdm master sub case table
    processCdmMasterSubCaseTable(sourcedb,sourcetable, targetdb, targettable, controltable)

    val applicationEndTime = LocalDateTime.now()
    logger.info("Cdm master sub case ETL process Completed : " + aplicationEndTime)

    val totalExecutionTime = Duration.between(applicationStartTime, applicationEndTime).toMillis
    logger.info("Total Execution Time in milliseconds : " + totalExecutionTime)
  }

  def processCdmMasterSubCaseTable(
                                  sourcedb: String,
                                  sourcetable: String,
                                  targetdb: String,
                                  targettable: String,
                                  controltable: String
                                  ): DataFrame = {

    // Drop data table anyway
    dropTableByName(targetdb, targettable, spark)

    //Create Data Frames
    val tableDF = getTableAsDataFrameWithCurrentRecords(sourcedb, sourcetable, spark)

    //save dataframe as table
    storeDataFrameAsParquet(tableDF : Any, targetdb, targettable)

    logger.info("Calculating statistical data")
    val tableRowCount = getNumberOfRowsInTable(targetdb,targettable,spark)

    // Get system date and time
    val date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val c = Calendar.getInstance()
    val cdcColumnMax = date.format(c.getTime())

    logger.info("Target Table Row Count : " + tableRowCount)
    logger.info("cdcColumnMax value : " + cdcColumnMax)
    insertStaticDataIntoControlTable(sourcedb,sourcetable,targetdb,targettable,controltable,cdcColumnMax,tableRowCount,spark)
  }

  def main(args: Array[String]): Unit = {
    //Define the source and destination tables to load from and to which are parameterized
    val environment = args(0)
    val sourcedb = args(1)
    val sourcetable = args(2)
    val targetdb = args(3)
    val targettable = args(4)

    initialize(environment, sourcedb, sourcetable, targetdb, targettable)
  }

}
*/
