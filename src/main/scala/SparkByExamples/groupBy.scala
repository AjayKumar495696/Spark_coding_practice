package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object groupBy {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val simpleData = Seq(
      ("James","Sales","NY",90000,34,10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    //df.show(false)
    val df1 = df.groupBy("department").sum("salary")
    df1.show(false)
    val df2 = df.groupBy("department").count()
    //df2.show(false)
    val df3 = df.groupBy("department").mean("salary")
    //df3.show(false)
    val df4 = df.groupBy("department","state")
      .sum("salary","bonus")
    //df4.show(false)
    val df5 = df.groupBy("department")
      .agg(
        sum("salary").as("Total_Salary"),
        avg("salary"),
        min("salary").as("MinSalary"),
        sum("bonus"),
        avg("bonus")
      )
    //df5.show(false)
  }

}
