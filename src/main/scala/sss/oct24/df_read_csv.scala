package sss.oct24

import org.apache.spark.sql.SparkSession

object df_read_csv extends App {
  //instialise spark session
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  //create data frame load data from csv file
  val df = spark.read.option("header", true).option("inferSchema", true)
    .csv("C:\\Users\\Niwas\\Downloads\\MOCK_DATA (2).csv")
    .show()


}
