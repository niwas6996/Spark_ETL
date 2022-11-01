package sss.oct24

import org.apache.spark.sql.SparkSession

object filter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .option("timestampFormat", "yyyy-MM-dd''HH:mm:ss")
      .option("mode", "failfast")
      .option("path", "C:\\Users\\Niwas\\Downloads\\survey.csv")
      .load()
    df.select("state", "self_employed").show()
    df.printSchema()
    df.count()
    df.columns.foreach(println)
    df.describe("care_options").show
    df.select("benefits", "care_options").distinct().show
    df.select("Country", "comments").distinct.show()
    //filter(" Age < 30 ").show


  }
}
