package sss.oct25

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.dense_rank

  object WindowFunction {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("WindowFunction").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("Error")
      val sc = spark.sparkContext
      import spark.implicits._

      val empDataRdd = sc.parallelize(Seq((11, "sreevidhya", "Bcm", 2000, 22),
        (21, "vidya", "kurnool", 3000, 25),
        (31, "kushi", "nellore", 4000, 24)))

      val depData = sc.parallelize(Seq((11, "Sreevidhya"),
        (21, "Sreevidhya"),
        (31, "kushi")
      ))
      val empDataDF = spark.createDataFrame(empDataRdd).toDF("dept_id", "name", "place", "salary", "age")
      val depDataDF = spark.createDataFrame(depData).toDF("dept_id", "name")

      empDataDF.show()
          val emp_dep_join = empDataDF.join(depDataDF,empDataDF("dept_id")===depDataDF("dept_id"),"inner")

          emp_dep_join.show()

          val windowSpec = Window.orderBy($"salary".desc)
          val resultDF = emp_dep_join.withColumn("salary_rank",dense_rank().over(windowSpec))
            .filter("salary_rank = 2")
          resultDF.show()




  }

}