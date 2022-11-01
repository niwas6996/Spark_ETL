package sss.oct25

import org.apache.spark.sql.SparkSession

object TrimFunction {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val data = Seq((1,"ABC    "), (2,"     DEF"),(3,"        GHI    ") )
    val df = data.toDF("col1","col2")
    df.show()
    import org.apache.spark.sql.functions.{trim,ltrim,rtrim,col}

    //using withColumn to remove white spaces
    df.withColumn("col2",trim(col("col2"))).show()
    df.withColumn("col2",ltrim(col("col2"))).show()
    df.withColumn("col2",rtrim(col("col2"))).show()

    //Using select to remove white spaces
    df.select(col("col1"),trim(col("col2")).as("col2")).show()

    //Using SQL Expression to remove white spaces
    df.createOrReplaceTempView("TAB")
    spark.sql("select col1,trim(col2) as col2 from TAB").show()

  }

}