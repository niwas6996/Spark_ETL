package sss.oct29

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object nareshInterview extends App {

  val spark = SparkSession.builder.master("local").getOrCreate()

  val schema = new StructType().add("date", StringType).add("user", StringType)
    .add("IP", StringType).add("host", StringType)

  val inputCsv = spark.read.option("delimiter", "|").schema(schema).csv("input.csv")
  val header = inputCsv.first()
  val finalDf = inputCsv.filter(_ != header)




}
