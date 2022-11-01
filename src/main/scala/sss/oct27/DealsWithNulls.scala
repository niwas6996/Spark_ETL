package sss.oct27

import org.apache.spark.sql.SparkSession

object DealsWithNulls {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DealsWithNulls").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val inputDf = spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\sreev\\Downloads\\MOCK_DATA (1).csv")
    inputDf.printSchema()
    inputDf.show()

    inputDf.na.fill(0)
      .na.fill("-",Seq("first_name"))
      .na.fill("-",Seq("last_name"))
      .na.fill("").show(false)
  }

}