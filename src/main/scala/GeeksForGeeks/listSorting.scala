package GeeksForGeeks

object listSorting {
  def main(args:Array[String]):Unit={
    val list:List[Int] = List(2,1,6,2,3,7,4,7,9,5,8)
    val sortedList:List[Int] = list.sorted
    println(list)
    println(sortedList)
  }

}
