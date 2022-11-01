package sss.oct29

object commonValueInSet {
  def main(args: Array[String]): Unit = {


    var set1: Set[Int] = Set(1, 2, 3, 3, 4);
    var set2: Set[Int] = Set(10, 20, 3);
    var set3 = set1 ++ set2
    var set4 = set1.++(set2)
    println(set3)
    println(set4)
    println(set1.intersect(set2));

  }
}