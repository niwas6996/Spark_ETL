package sss.oct23

import org.apache.spark.sql.SparkSession

object joins extends App {
  val spark = SparkSession.builder().master("local[1]").getOrCreate()

  val sr = "C:\\Users\\Niwas\\Downloads\\data-djjeIm9idDKKnMrSDsIgP.csv"
  val df1 = spark.read.option("inferSchema", "true").option("header", "true").csv(sr).show(truncate = false)
  val sc = "C:\\Users\\Niwas\\Downloads\\data--j6txdxBmG-90qglWuJhO.csv"
  val df2 = spark.read.option("inferSchema", "true").option("header", "true").csv(sc).show(truncate = false)

}
