package sss.oct27

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, upper}

object LowerCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LowerCase").master("local[*]").getOrCreate()
    val df = spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\sreev\\Downloads\\MOCK_DATA (1).csv")
    df.show()
    val df1 = df.withColumn("first_name1",lower(col("first_name")))
    val df2 = df.withColumn("last_name2",upper(col("last_name")))
    df1.show()
    df2.show()
  }

}