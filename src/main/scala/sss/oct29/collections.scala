package sss.oct29

object collections {
  def main(args: Array[String]) {
    var list:List[String] = List("Scala","Java","Python","C++")
    for ( x <- list ) {
      println( x )
      println(list.length)
      println(list.reverse)
      println(list.sorted)
      println(list.indexOf("Java"))
      println(list.toString())
      println(list.head)
      println(list.tail)
      println(list.isEmpty)
      
      var list1:List[String] = List("Scala","Java")
      var list2:List[String] = List("Python","C++")

      println(list1:::list2)
      println(List.concat(list1,list2))
      println(list1.:::(list2))

    }
  }

}
