package GeeksForGeeks
import scala.io.StdIn.readInt
object leapYear {
  def main(args:Array[String]):Unit={
    val a = List(1,4,7,8,3,5,9,2)
    println(findMax(a))

    def findMax(k:List[Int]):Int={
      val b = k.max
      return b
    }
  }
}