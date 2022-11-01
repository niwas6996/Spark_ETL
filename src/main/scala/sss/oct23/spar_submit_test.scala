package sss.oct23

import org.apache.spark.sql.SparkSession

object spar_submit_test extends App {

  val spak = SparkSession.builder().master("local")
    .appName("spark_submit_test").getOrCreate()
  val path = "C:\\Users\\Niwas\\Downloads\\MOCK_DATA (1).csv"
  val input_cv = spak.read.option("header", true).option("inferSchema", true)
    .csv(path)
  val path1 = "C:\\Users\\Niwas\\Downloads\\MOCK_DATA (2).csv"
  val input_cv1 = spak.read.option("header", true).option("inferSchema", true)
    .csv(path1)
  input_cv.join(input_cv1, input_cv("id") === input_cv1("id"), "inner")
  val input_cv2=input_cv.groupBy().pivot("gender")
  input_cv.show()








  //  spak.sparkContext.stop()


}
