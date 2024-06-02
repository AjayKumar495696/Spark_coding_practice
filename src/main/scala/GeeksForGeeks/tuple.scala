package GeeksForGeeks
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object tuple {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit= {
      val spark:SparkSession = SparkSession.builder()
        .master("local[1]").appName("SparkByExamples.com").getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1,"Ajay",88),
      (2,"Sandeep",90),
      (3,"Goutham",90),
      (4,"Sravanthi",85))
    val cols = Seq("Id","Name","Marks")
    val df = data.toDF(cols:_*)
    //df.printSchema()
    //df.show(false)
    df.createOrReplaceTempView("Asection")
    spark.sql("select Id, Name, Marks, rank() over(order by Marks desc) as Rank, dense_rank() over(order by Marks desc) as Denserank, row_number() over(order by Marks desc) as Rownumber from Asection").show(false)
  }
}
