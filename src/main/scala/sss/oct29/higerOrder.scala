package sss.oct29

object higerOrder  {
 // val spark=SparkSession.builder().master("local[1]").getOrCreate()
  def main(args: Array[String]): Unit = {
    higherOrderFunction(add)
  }

  def higherOrderFunction(f:(Int,Int)=>Int):Unit = {
    println(f(11,11))
  }

  def add(a:Int,b:Int):Int = {
    a+b
  }




//def main(args: Array[String]): Unit = {
//
//  val add = higherOrderFunction()
//  add(1,4)
//  }
//
//  def higherOrderFunction()= {
//  (a:Int,b:Int) => println(a+b)
//  }

  }
