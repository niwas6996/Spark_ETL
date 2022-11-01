package sss.oct24

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object column_changes extends App {
  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  import spark.implicits._
  val data = Seq(("James;","","Smith","36636","M","3000"),
    ("Robert","","Williams","42114","M","4000"),
    ("Maria","Anne","Jones","39192","F","4000"),
    ("Jen","Mary","Brown","","F","-1"),
  ("Michael","Rose","","40288","M","4000"))

  val schema = new StructType()
    .add("name",StringType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
    .add("dob",StringType)
    .add("gender",StringType)
    .add("salary",StringType).toDF()













//  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
//  //chaining to operate on multiple columns
//  df.withColumn("Country", lit("USA"))
//    .withColumn("anotherColumn",lit("anotherValue"))



}
