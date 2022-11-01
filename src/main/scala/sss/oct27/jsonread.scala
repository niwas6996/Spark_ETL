package sss.oct27

import org.apache.spark.sql.SparkSession

object jsonread extends  App {
  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  val firstDF = spark.read.format("json").option("inferSchema", true).option("multiline",true)
    .load("C:\\Users\\Niwas\\IdeaProjects\\untitled\\mocked_test_data.json")
    firstDF.show()
//  firstDF.printSchema()
//firstDF.take(10).foreach(println)
//  val longType=LongType



}
