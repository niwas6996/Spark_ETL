package sss.oct27

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
      .select("memberid", "phone_number")

    val df2 = df1.select("memberid", "phone_number.*")
      .groupBy($"memberid").pivot($"phone_type").agg(functions.max($"phone_number"))
      .orderBy($"memberid")

    df2.show()
    df2.coalesce(1).write.mode("overwrite").option("header",true).csv("c:\\Users\\Niwas\\Desktop\\new.csv")
  }
}