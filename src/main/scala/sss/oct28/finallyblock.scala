package sss.oct28

object finallyblock {
  def divide(first:Int, second:Int) = {
    try{
      first/second
    }catch{
      case e: ArithmeticException => println(e)
    }
    finally {
      println("Finally block")
    }
    println("executing code after exception")
  }
  def main(args:Array[String]){

    divide(100,0)

  }


}
