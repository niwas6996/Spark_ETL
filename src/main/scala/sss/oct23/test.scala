package sss.oct23

import org.apache.spark.sql.SparkSession

object test extends App {
  val spark = SparkSession.builder().master("local[1]").getOrCreate() //enableHiveSupport().getOrCreate()
  val df = spark.createDataFrame(
    List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000)))
  //df.show()
  df.createOrReplaceTempView("sample_table")
  val df2 = spark.sql("SELECT _1,_2 FROM sample_table")
  //df2.show()
  //crete hive table and query it
  //  spark.table("sample_table").write.saveAsTable("sample_hive_table")
  //  val df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
  //  df3.show()

}
