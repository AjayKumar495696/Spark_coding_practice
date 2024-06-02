package GeeksForGeeks

class child {
  def func(a:String,b:String): Unit = {
    println("My name is "+a)
    println("I am from "+b)
  }
  def func(a:Int,b:Int): Unit = {
    println("His Age is "+a)
    println("His height is "+b)
  }
  def func(a:String,b:Int): Unit = {
    println("This city is "+a)
    println("Its position is "+b)
  }
}

object details {
  def main(args:Array[String]):Unit={

    var obj = new child()
    obj.func("Laxman","Hyderabad")
    obj.func(20,5)
    obj.func("Bangalore",2)
  }
}
