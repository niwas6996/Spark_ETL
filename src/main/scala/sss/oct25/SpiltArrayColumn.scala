package sss.oct25

import org.apache.spark.sql.SparkSession

object SpiltArrayColumn extends  App{
  val spark = SparkSession.builder().master("local").appName("SpiltArrayColumn").getOrCreate()
  import spark.implicits._

  val targets = Seq(("Plain Donut",Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
  val df = spark.createDataFrame(targets).toDF("Name","Prices")
  df.show()
  df.printSchema()

  val df1 = df.select($"Name",
  $"Prices"(0).as ("Low Price"),
    $"Prices"(0).as ("Low Price"))
}