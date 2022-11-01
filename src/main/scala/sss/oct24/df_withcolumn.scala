package sss.oct24

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object df_withcolumn {
  def main(args: Array[String]): Unit = {

  }
//instialise spark session
  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  //create dataframe load data from csv file
  val df = spark.read.option("header", true).option("inferSchema", true)
    .csv("C:\\Users\\Niwas\\Downloads\\MOCK_DATA (2).csv")
  //add new column with new name
  val df2 = df.withColumn("fname", col("first_name"))
    .withColumn("lname", col("last_name"))
    .withColumn("gmail", col("email")).show(truncate = false)
  //
  //  val df2 = df.withColumn("fname",$"first_name")
  //    .withColumn("lname",$"last_name")
  //    .withColumn("gmail",$"email").show()

}
