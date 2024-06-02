package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object createDataFrame {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val columns = Seq("Name","Age")
    val data = Seq(("Ajay",25),("Laxman",20))
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD = rdd.toDF()
    //dfFromRDD.printSchema()
    //dfFromRDD.show(false)
    val dfFromRDD1 = rdd.toDF("NAME","AGE")
    //dfFromRDD1.printSchema()
    //dfFromRDD1.show(false)
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
    //dfFromRDD2.printSchema()
    //dfFromRDD2.show(false)
    val df1 = spark.read.option("header","true").csv("C:\\A_LEARNING\\SPARK_LEARNING\\CricketersList.csv")
    //df1.printSchema()
    //df1.show(false)
    val df2 = spark.read.text("C:\\A_LEARNING\\SPARK_LEARNING\\HeroesList.txt")
    //df2.printSchema()
    //df2.show(false)
  }
}
