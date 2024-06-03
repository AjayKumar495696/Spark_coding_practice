package GeeksForGeeks
//import scala.collection.immutable._
//import scala.collection.mutable.ListBuffer
//import Array._

object list {
  def main(args:Array[String]):Unit={
    /*val list1 : List[Int] = List(1,2,3,4,5)
    println(list1)
    println("")
    for (a <- list1) {
      println(a)
    }*/


    /*val a = List(1,2,3,4,5)
    println(a.reverse)*/

    /*var a = "Hello World"
    println(a)
    println(a.reverse)*/

    //var a = ListBuffer("C","Java","C++","Scala")
   /* a -= "C"
    a -= ("C++","Scala")*/
    //a.remove(1,3)
    //println(a)
    //println(a.remove(0))

    /*var a = Map("Ajay" -> 1,"Bharath" -> 2, "Vilas" -> 3)
    var b = a("Ajay")
    println(b)*/

   /* var arr1 = Array(1,2,3,4)
    var arr2 = Array(5,6,7,8)
    var arr3 = concat(arr1,arr2)
    for (i <- arr3) {
      println(i)
    }*/

    /*val rows = 2
    val cols = 3
    val names = Array.ofDim[String](rows,cols)
    names(0)(0) = "Hi"
    names(0)(1) = "There"
    names(0)(2) = "Laxman"
    names(1)(0) = "How"
    names(1)(1) = "Are"
    names(1)(2) = "You"

    for {
      i <- 0 until rows
      j <- 0 until cols
    }
      println(s"($i) ($j) = ${names(i)(j)}")*/

    var a = Array(1,2,7,8)

    var b = a ++ Array(4,5,9,10)
    for (i <- b) {
      println(i)
    }

  }

}
