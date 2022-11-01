package sss.oct24

import org.apache.spark.sql.SparkSession

object pivot extends App {
  val spark=SparkSession.builder().master("local[1]").getOrCreate()

  //  passing seq of values
  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))



  import spark.sqlContext.implicits._
  //convert data to dataframe
  val df = data.toDF("Product","Amount","Country")
  df.show()
  //reduce the length of compile error lines
  spark.sparkContext.setLogLevel("ERROR")
  //applied some trasformations on derived dataframe
  val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
  //applied action on base code
  pivotDF.show()
  val countries = Seq("USA","China","Canada","Mexico")

  val pivotDF1 = df.groupBy("Product","Country")
     .sum("Amount")
     .groupBy("Product")
     .pivot("Country")
     .sum("sum(Amount)")
  pivotDF1.show()

}
