package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object CodingTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String])={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val marksDF = spark.read.option("header","true").csv("C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\StudentsMarks.csv")
    //marksDF.printSchema()
    //marksDF.show(false)

    marksDF.createOrReplaceTempView("STUDENTS")
    val outputDF = spark.sql("Select Id,Name,Marks, rank() over(order by Marks desc) as Rank, dense_rank() over(order by Marks desc) as DenseRank, row_number() over(order by marks desc) as RowNumber from STUDENTS")
    //outputDF.printSchema()
    //outputDF.show(false)
    //outputDF.write.option("header","true").csv("C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\outputStudentsMarks2.csv")
    //val dfCSV = spark.read.option("header", "true").csv("C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\outputStudentsMarks2.csv")
    //dfCSV.printSchema()
    //dfCSV.show(false)
    //outputDF.write.option("header","true").parquet("C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\outputStudentsMarks3.parquet")
    val dfParquet = spark.read.option("header", "true").parquet("C:\\Users\\ajays\\OneDrive\\Desktop\\New_Job_Preperation_2024\\Learning_BigData\\outputStudentsMarks3.parquet")
    dfParquet.show(false)
  }
}
