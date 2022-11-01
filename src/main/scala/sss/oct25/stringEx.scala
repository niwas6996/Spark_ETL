package sss.oct25

object stringEx extends App {
  val str = "my name is srinivas"
  //  println(str.substring(1))
  //  println(str.charAt(0))
  println(reverseString(str))

  def reverseString(s: String): String = {
    // if (s.isEmpty) “”
    if (s.length == 1) {
      s
    } else {
      //println(reverseString(s.substring(1)) + s.charAt(0))
      reverseString(s.substring(1)) + s.charAt(0)
      //else reverseString(s.tail) + s.head
    }
  }
}



//
//  def reverseString (newString:String):String = {
//
//
//    var revString = "my name is srinivas"
//    val n = newString.length()
//    for (i <- 0 to n - 1)
//      revString = revString.concat(newString.charAt(n - i - 1).toString)
//  } }

