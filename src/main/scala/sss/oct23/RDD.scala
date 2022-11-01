package sss.oct23

import org.apache.spark.sql.SparkSession

object RDD extends App {
  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
  val rddCollect = rdd.collect()
  println("Number of Partitions" + rdd.getNumPartitions)
  println("Action First element" + rdd.first())
  println("Action RDD converted to Array[Int]")
  rddCollect.foreach(println)
  val rdd1 = spark.sparkContext.textFile("C:\\Users\\Niwas\\Desktop\\my.txt.txt")
  rdd1.foreach(f => {
    println(f)
  })
  val rddWhole = spark.sparkContext.wholeTextFiles(
    "C:\\Users\\Niwas\\Desktop\\my.txt.txt,C:\\Users\\Niwas\\Desktop\\NAME.TXT.txt")
  //  println("println(p._1+\"=>\"+p._2)")
  rddWhole.foreach(p => {
    println(p._1 + "=>" + p._2)
  })
  val rdd5 = spark.sparkContext.textFile("C:\\Users\\Niwas\\Desktop\\my.txt.txt")
  val rdd6 = rdd5.map(f => {
    f.split(",")
  })
  println("println(\"Col1:\"+f(0)+\",Col2:\"+f(1))")
  rdd6.foreach(f => {
    println("Col1:" + f(0) + ",Col2:" + f(1))
  })

}
