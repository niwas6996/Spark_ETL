package sss.oct29

object option {
  def main(args: Array[String]): Unit = {
    var map1:Map[Int,String]= Map(1->"one",2->"two",3->"three")
    println(map1.get(1));
    println(map1.get(1).get)
    println(map1.get(5).getOrElse("Not found"))
  }

}
