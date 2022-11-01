package sss.oct27

import org.apache.spark.sql.SparkSession

object png extends App{


  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  val df = spark.read.format("binaryFile").load("C:\\Users\\Niwas\\Downloads\\fontawesome-webfont.bin")
  df.printSchema()
  df.show()

  //"C:\Users\Niwas\Pictures\360_F_133868401_tDJ9AHcKRQZ8bogSTF0xk2E53E5IekmO.jpg"

}
