package sss.oct23

import org.apache.spark.sql.SparkSession

object newtest extends App {

  case class Schema(id: Int, dept: Long, type_cd: String, email_int: String = null, text_ind: String = null, push_ind: String = null)

  val spark = SparkSession.builder().master("local[1]").getOrCreate()

  import spark.implicits._

  val cols = Seq[Schema]().toDF.schema
  val data = Seq((87654321, 123456789, "LEVEL2", "Y", "", ""),
    (87654321, 123456789, "LEVEL1", "Y", "", ""),
    (87654321, 123456789, "LEVEL1", "", "Y", ""),
    (87654321, 123456789, "LEVEL1", "", "", "Y"))

  val df = spark.createDataFrame(data)
  //  val df2 = df.toDF(cols:_*)
  df.show()


}
