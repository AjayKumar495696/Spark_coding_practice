package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.types.{StringType,StructType}
import org.apache.spark.sql.functions.{lit,col}

object withColumn {
Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val data = Seq(
      Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
      Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
      Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
    )

    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
    //df.printSchema()
    //df.show(false)
    val df1 = df.where("salary == '4000'")
    //df1.show(false)
    val df2 = df.withColumn("country",lit("USA")).withColumn("anotherColumn",lit("anotherCountry"))
    //df2.show(false)
    val df3 = df.withColumn("salary",col("salary")*10)
    //df3.show(false)
    val df4 = df.withColumn("salary",col("salary").cast("String"))
    //df4.show(false)
    //df4.printSchema()
    //df.createTempView("PERSON")
    //spark.sql("select * from PERSON where salary=4000 ").show(false)
    val df5 = df.withColumnRenamed("salary","price")
    //df5.show(false)
    val df6 = df.drop("dob")
    //df6.show(false)
  }
}
