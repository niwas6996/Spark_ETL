package sss.oct29

object clousure {
  var increment  = 1
  val add = (i:Int) => i + increment
  def main(args: Array[String]) {
    println( add(2) )
  }
}
