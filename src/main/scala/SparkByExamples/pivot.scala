package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object pivot {
Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    /*val data = Seq(
      ("Banana",1000,"USA"),
      ("Carrots",1500,"USA"),
      ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"),
      ("Orange", 2000, "USA"),
      ("Banana", 400, "China"),
      ("Carrots", 1200, "China"),
      ("Beans", 1500, "China"),
      ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"),
      ("Carrots", 2000, "Canada"),
      ("Beans", 2000, "Mexico")
    )*/

    /*val df = data.toDF("Product","Amount","Country")
    df.show(false)
    val df1 = df.groupBy("Product").pivot("Country").sum("Amount")
    df1.show(false)
    val df2 = df.groupBy("Country").pivot("Product").sum("Amount")
    df2.show(false)*/

    val simpleData = Seq(
      ("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    //df.printSchema()
    //df.show(false)

    val simpleData2 = Seq(
      ("James", "Sales", "NY", 90000, 34, 10000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df2 = simpleData2.toDF("employee_name","department","state","salary","age","bonus")
    //df2.printSchema()
    //df2.show(false)

    val df3 = df.union(df2)
    df3.show(false)

    val df4 = df.union(df2).distinct()
    df4.show(false)
  }
}
