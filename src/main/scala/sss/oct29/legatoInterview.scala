package sss.oct29

import org.apache.spark.sql.SparkSession

object legatoInterview extends App  {

  val spark=SparkSession.builder().master("local[1]").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val data1 = Seq(("Table-1", 12), ("Table-2", 15), ("Table-3", 18))
  val data2 = Seq(("Table-1", 12), ("Table-6", 15), ("Table-3", 10))

  val cols = Seq("tableName", "rowCount")

  val dataset_1 = data1.toDF(cols: _*)
  val dataset_2 = data2.toDF(cols: _*)
  dataset_1.createTempView("dataset1")
  dataset_2.createTempView("dataset2")
  //  dataset_1.join(dataset_2,dataset_1("tableName")===dataset_2("tableName") and(dataset_1("rowCount")=!=dataset_2("rowCount")),"full")
  //    .withColumn("diff",dataset_1("rowCount")-dataset_2("rowCount"))
  dataset_1.join(dataset_2, Seq("tableName"), "full")
    .toDF("tableName", "Ds1_rowCount", "Ds2_rowCount")
    //  .na.fill(0,Array("Ds1-rowCount","Ds2-rowCount"))
    .createTempView("temp")
  //  .filter("where rowCount <> rowCount ")
  //  .filter($"Ds1-rowCount"=!=$"Ds2-rowCount")
  //    .show()
  spark.sql(
    """with cte as (select *,case when Ds1_rowCount is null then 0 else Ds1_rowCount end as Ds1,
      |case when Ds2_rowCount is null then 0 else Ds2_rowCount end as Ds2
      | from temp)
      | select tableName,Ds1_rowCount,Ds2_rowCount from cte where Ds1<>Ds2 order by 1
      | """.stripMargin).show()
  //spark.sql(
  //  """select * from temp where
  //    | (case when Ds1_rowCount is null then 0 end) != (case when Ds2_rowCount is null then 0 end)
  //    |""".stripMargin).show()
  //  spark.sql("""select * from dataset1 d1
  //              |full outer JOIN
  //              |dataset2 d2
  //              |on
  //              |d1.tableName=d2.tableName
  //              |where d1.rowCount<>d2.rowCount
  //              |order by d1.tableName""".stripMargin).show()


  //spark.sql("""select * from data_set1 d1 union all select * from data_set2 d2 """ ).show()

}