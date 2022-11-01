package sss.oct29

import org.apache.spark.sql.SparkSession

object typeInference extends App{
  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  var first:String = "srinivas, "
  var second:String = "bigdata"
  var third = first + second
  //the compile infers that third is of type String

}
