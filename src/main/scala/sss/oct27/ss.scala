package sss.oct27

import org.apache.spark.sql.SparkSession

object ss extends App{
  //sparksession intialigation
  val spark = SparkSession.builder()
         .master("local[1]")
    .getOrCreate()
//create RDD with parallize method
  val x = spark.sparkContext.parallelize(Array("b", "a", "c"))
  //apply map on rdd
  val y = x.map(z => (z,1))
  println(x.collect().mkString(", "))
  println(y.collect().mkString(", "))

}

