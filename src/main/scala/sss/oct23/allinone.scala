package sss.oct23

import org.apache.spark.sql.SparkSession

object allinone {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    //spark.sparkContext.setloglevel("ERROR")
    import spark.implicits._

  }
}


