package sss.oct24

import org.apache.spark.sql.SparkSession

object column_operations extends App {
  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  val df=spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\Niwas\\Downloads\\MOCK_DATA (2).csv")
//  df.createOrReplaceTempView("emp")
//  spark.sql("""""")
  //chaining to operate on multiple columns


}
