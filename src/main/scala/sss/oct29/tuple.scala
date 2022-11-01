package sss.oct29

object tuple {
  def main(args: Array[String]) {
    val tuple1 = (1, 2);
    val tuple2 = Tuple3(1, 2, 3);
    println(tuple1._1 + tuple1._2)
    println(tuple2._1 + tuple2._2)
    tuple2.productIterator.foreach { i =>
      println(i)
      println(tuple2.toString())

    }
  }
}


