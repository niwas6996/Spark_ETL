package sss.oct24

import org.apache.spark.sql.SparkSession

object battles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val df = spark.read.option("header", true).option("inferSchema", true)
      .csv("C:\\Users\\Niwas\\Downloads\\battles.csv")
        df.createOrReplaceTempView("battles")
    df.groupBy("year").pivot("region")
    df.show()

//    df.printSchema()
//    df.select("name", "year", "battle_number", "attacker_king",
//      "defender_king", "attacker_outcome", "attacker_commander",
//      "defender_commander", "location").filter("year == 298")
//    df.printSchema()
//    df.createOrReplaceTempView("battles")


    val df1 = spark.read.option("header", true).option("inferSchema", true)
      .csv("C:\\Users\\Niwas\\Downloads\\character-deaths.csv").show()
//    df1.printSchema()
//    df1.select("Name", "Allegiances", "Death Year", "Book of Death", "Death Chapter",
//      "Book Intro Chapter", "Gender", "Nobility", "GoT", "CoK", "SoS", "FfC", "DwD")

    //df1.createOrReplaceTempView("deaths")
//    val df3 = df.sqlContext.sql("select attacker_1,count(distinct(' ')) from battles group by attacker_1")
//      .show()





  }

}
