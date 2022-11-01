package sss.oct27

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object RangePartition {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").getOrCreate()
    val data=Seq((1,20),(2,10),(3,10),(5,60),(6,70),(8,70),(4,70),(9,90))
    import spark.implicits._
    val datacolumns=Seq("class","numofstudents")
    val df=data.toDF(datacolumns:_*)
    val df2 =df.repartitionByRange(5,col("numofstudents")).rdd.getNumPartitions
    println(df2)

  }

}