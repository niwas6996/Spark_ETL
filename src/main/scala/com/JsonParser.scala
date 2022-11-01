package com

import org.apache.spark.sql.functions.{arrays_zip, explode}
import org.apache.spark.sql.{SparkSession, functions}


object JsonParser {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.appName("interview").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.read.option("multiline", true).json("C:\\Users\\Niwas\\IdeaProjects\\untitled\\input.json")
    df.show()

    df.printSchema()


    val df1 = df.withColumn("phone_number", explode(arrays_zip($"phone_type", $"phone_number")))
              .select("memberid","phone_number")

    val df2 = df1.select("memberid","phone_number.*")
               .groupBy($"memberid").pivot($"phone_type").agg(functions.max($"phone_number"))
                 .orderBy($"memberid")

        df2.show()

    //    +--------+------------+----------+
    //    |memberid|phone_number|phone_type|
    //    +--------+------------+----------+
    //    |       1|      [1, 4]|    [M, F]|
    //      |       2|         [3]|       [F]|
    //      |       3|      [2, 5]|    [M, T]|
    //      |       4|      [6, 7]|    [F, T]|
    //      |       5|   [9, 0, 8]| [M, F, T]|
    //      +--------+------------+----------+


    /*    expected:
          memberid   M       F    T
           1          1       4    NULL
          2          NULL    3    NULL
          3          2      NULL   5
        4          NULL    6     7
        5          9       0     8*/

  }
}
