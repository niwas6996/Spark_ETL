package sss.oct23

import org.apache.spark.sql.SparkSession

object selfjoin extends App {
  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  val emp = Seq((1, "Smith", 1, "10", 3000),
    (2, "Rose", 1, "20", 4000),
    (3, "Williams", 1, "10", 1000),
    (4, "Jones", 2, "10", 2000),
    (5, "Brown", 2, "40", -1),
    (6, "Brown", 2, "50", -1)
  )
  val empColumns = Seq("emp_id", "name", "superior_emp_id", "emp_dept_id", "salary")

  import spark.sqlContext.implicits._

  val empDF = emp.toDF(empColumns: _*)
  val selfDF = empDF.as("emp1").join(empDF.as("emp2"),
    $"emp1.superior_emp_id" === $"emp2.emp_id", "inner")


  selfDF.select($"emp1.emp_id", $"emp1.name",
    $"emp2.emp_id".as("superior_emp_id"),
    $"emp2.name".as("superior_emp_name")).show()

  selfDF.createOrReplaceTempView("EMP")

  spark.sql("""select e.* from EMP e join EMP e1 on e.superior_emp_id=e1.emp_id """).show()

}
