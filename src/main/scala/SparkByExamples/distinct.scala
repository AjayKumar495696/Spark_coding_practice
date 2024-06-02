package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object distinct {
Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val simpleData = Seq(
      ("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name","department","salary")
    //df.show(false)
    val df1 = df.distinct()
    //println("Distinct Count: "+df1.count())
    //df1.show(false)
    val df3 = df.dropDuplicates()
    //df3.show(false)
    val df4 = df.dropDuplicates("department","salary")
    df4.show(false)
  }
}
