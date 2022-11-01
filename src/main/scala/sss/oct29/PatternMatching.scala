package sss.oct29

object PatternMatching {
  def main(args: Array[String]): Unit = {
    val x: Int=4
    var res= x match {
      case 1 => "one"
      case 2 => "two"
      case 3 => "three"
      case _ => "other"
    }
    println(res)
    println( res(3))
    println( res(1) )
  }
}
