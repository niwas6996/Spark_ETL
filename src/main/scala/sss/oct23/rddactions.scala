package sss.oct23

import org.apache.spark.sql.SparkSession

object rddactions extends App {
  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)))


  val listRdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 3, 2))

  var sum = 0
  for (i <- listRdd) {
    sum += i
  }
  println("sum: " + sum)


  //  println(add(listRdd))

  def param0 = (accu: Int, v: Int) => accu + v

  def param1 = (accu1: Int, accu2: Int) => accu1 + accu2

  println("aggregate : " + listRdd.aggregate(0)(param0, param1))
  println(listRdd.aggregate(1)(param0, param1))

}
