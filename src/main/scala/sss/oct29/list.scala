package sss.oct29

import org.apache.spark.sql.SparkSession

object list extends App{
  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  val myList = List(1,2,3,4,5,6,7)

  val myEvenList = myList.filter((n: Int) => n % 2 == 0)
  //List(2,4,6)

  val myOddList = myList.filter((n:Int) => n % 2 != 0)
  //List(1,3,5,7)

}
