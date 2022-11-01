package sss.oct27

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object DfColumnWithUDF extends App {
  val spark = SparkSession.builder().appName("DfColumnWithUDF").master("local").getOrCreate()

  import spark.implicits._

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut" => Seq(100, 500)
    case "vanilla donut" => Seq(200, 400)
    case "glazed donut" => Seq(300, 600)
    case _ => Seq(150, 150)
  }
  val udfStockMinMax = udf(stockMinMax)
  val df2 = df.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
  df2.show()
}
