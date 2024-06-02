package GeeksForGeeks
import scala.util.control.Breaks.breakable
import scala.util.control.Breaks.break

object controlStatements {
  def main(args:Array[String]):Unit={
    /*var a = 30
    if (a > 31) {
      println("Given condition is true")
    }
    else {
      println("It is wrong")
    }*/

    /*var a = 1000
    var b = 30000
    var c = 40000
    if (a > b) {
      if (a > c) {
        println("1. a is the largest one "+a)
      }
      else {
        println("2. c is the largest one "+c)
      }
    }
    else {
      if (b > c) {
        println("3. b is the largest one "+b)
      }
      else {
        println("4. c is the largest one "+c)
      }
    }*/

    /*var a = 70
    if (a == 100) {
      println("a is 100")
    }
    else if (a == 60) {
      println("a is 60")
    }
    else if(a == 50) {
      print("a is 50")
    }
    else {
      println("no match found")
    }*/

    /*var x = 1
    while (x <= 1) {
      println("Value of x is : "+x);
      x += 1;
    }*/

    /*var y = 1
    do {
      println("Value of y is : "+y)
      y += 1
    } while(y <= 5)*/

    // var a = 0
    /*for (a <- 1 to 20) {
      println(s"$a table :")
      for (b <- 1 to 10) {
        println(s"$a * $b = "+(a*b))
      }
      println()
    }*/

    /*var a = 5
    var b = 0
    while (a < 7) {
      b = 0
      while ( b < 7) {
        println("The values are : a = "+a," b = "+b);
        b += 1;
      }
      println()
      a += 1;
    }*/

    /*var a = 1
    var b = 1
    while(a <= 20) {
      b = 1
      println(s"$a table :")
      while (b <= 10) {
        println(s"$a * $b = " + (a * b))
        b += 1
      }
      println()
      a += 1
    }*/

    /*breakable {
       for (a <- 1 to 10) {
         if (a == 6)
           break
         else {
           println(a)
         }
       }
    }*/
    /*println("A")
    println("B\n\n\n\n")*/

    val x = "Hi" +
      "There" +
      "Who are" +
      "you"
    val y =
      """I am
        |Ajay.
        |I am 25.
        |""".stripMargin
        println(x)
    println(y)
  }
}
