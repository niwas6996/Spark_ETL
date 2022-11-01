package sss.oct23

import org.apache.spark.sql.SparkSession

object practicex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    //    val sr="C:\\Users\\Niwas\\Desktop\\m.csv.csv"
    //     val df=spark.read.option("header","true").option("inferSchema","true").csv(sr)
    //
    //    df.createOrReplaceTempView("table")
    //    spark.sql(
    //      """select * from table where\ plan = 'VSN' and id in
    //        |(select id from table group by id having count(distinct plan) = 1 )""".stripMargin).show()

    spark.sparkContext.setLogLevel("ERROR")
    //      val sr="C:\\Users\\Niwas\\Desktop\\m.csv.csv"
    //      val df=spark.read.option("header",true).option("inferSchema",true).csv(sr)
    //      df.show()
    //      df.createOrReplaceTempView("tab")
    //
    //
    //      spark.sql(
    //        """select * from tab where plan = 'VSN' and id in
    //          | (select id from tab group by id having count(distinct plan)=1)
    //          |""".stripMargin).show()

    val st = "C:\\Users\\Niwas\\Desktop\\m.csv.csv"
    val inputdf = spark.read.option("inferSchema", "true").option("header", "true").csv(st).show(truncate = false)

    //    inputdf.where(col("id"))=!= ""


  }


}


