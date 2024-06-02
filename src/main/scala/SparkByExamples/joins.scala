package SparkByExamples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object joins {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    import spark.implicits._

    val emp = Seq(
      (1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(
      ("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )
    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.show(false)
    val dfLeft = empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"left")
    //dfLeft.show(false)
    val dfRight = empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"right")
    //dfRight.show(false)
    val dfLeftSemi = empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"leftsemi")
    //dfLeftSemi.show(false)
    val dfLeftAnti = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti")
    //dfLeftAnti.show(false)
    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")
    val df1 = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    df1.show(false)
  }

}
