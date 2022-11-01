package sss.oct23

import org.apache.spark.sql.SparkSession

object wordcount extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()

  val sc = spark.sparkContext
  val rdd1 = sc.textFile("C:\\Users\\Niwas\\Desktop\\NAME.TXT.txt")

  val rdd2 = rdd1.flatMap(f => f.split(" "))
  val rdd3 = rdd2.map(m => (m, 1))
  val rdd4 = rdd3.filter(a => a._1.startsWith("a"))
  val rdd5 = rdd3.reduceByKey(_ + _)
  val rdd6 = rdd5.map(a => (a._2, a._1)).sortByKey().foreach(println)
  //  rdd6.foreach(println)


}
