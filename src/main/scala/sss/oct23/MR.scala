package sss.oct23

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.Inner

object MR extends App {

  val spark = SparkSession.builder().master("local").getOrCreate()
  val emp = Seq((1, "Smith", "10"),
    (2, "Rose", "20"),
    (3, "Williams", "10"),
    (4, "Jones", "10"),
    (5, "Brown", "40"),
    (6, "Brown", "50")
  )
  val empColumns = Seq("emp_id", "name", "emp_dept_id")

  import spark.sqlContext.implicits._

  val empDF = emp.toDF(empColumns: _*)
  empDF.show(false)
  val dept = Seq(("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )
  val deptColumns = Seq("dept_name", "dept_id")
  val deptDF = dept.toDF(deptColumns: _*)
  //deptDF.show(false)


  val address = Seq((1, "1523 Main St", "SFO", "CA"),
    (2, "3453 Orange St", "SFO", "NY"),
    (3, "34 Warner St", "Jersey", "NJ"),
    (4, "221 Cavalier St", "Newark", "DE"),
    (5, "789 Walnut St", "Sandiago", "CA")
  )
  val addColumns = Seq("emp_id", "addline1", "city", "state")
  val addDF = address.toDF(addColumns: _*)
  //addDF.show(false)
  //joins
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    .join(addDF, empDF("emp_id") === addDF("emp_id"), "inner")
    .show(false)
  //inner.sql
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), Inner.sql)
    .join(addDF, empDF("emp_id") === addDF("emp_id"), Inner.sql)
    .show(false)
  //using where
  empDF.join(deptDF).where(empDF("emp_dept_id") === deptDF("dept_id"))
    .join(addDF).where(empDF("emp_id") === addDF("emp_id"))
    .show(false)
  //using filter condition
  empDF.join(deptDF).filter(empDF("emp_dept_id") === deptDF("dept_id"))
    .join(addDF).filter(empDF("emp_id") === addDF("emp_id"))
    .show(false)

  //temparory tables
  empDF.createOrReplaceTempView("EMP")
  deptDF.createOrReplaceTempView("DEPT")
  addDF.createOrReplaceTempView("ADD")
  spark.sql("""select * from EMP e join DEPT d on e.emp_dept_id=d.dept_id join add a on e.emp_id=a.emp_id """).show()
}
