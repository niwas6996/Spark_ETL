import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object tg extends App {

  //val spark=SparkSession.builder().master("local[1]").getOrCreate()


  val spark = SparkSession.builder().master("local[1]")
    .appName("srinujson")
    .getOrCreate()
  val simple_json = """{"parent_tag":[{"seq":1,"id":2,"value":"Chennai"},{"seq":2,"id":5,"value":"Shanmu"}]}"""

  import spark.implicits._

  val schema = StructType(List(
    StructField("seq", IntegerType, true),
    StructField("id", IntegerType, true),
    StructField("value", StringType, true)))

  val inputDf = spark.read.json("C:\\Users\\Niwas\\IdeaProjects\\spark-etl-1\\src\\main\\resources\\input\\test.json")
  inputDf.printSchema()
  inputDf.show(false)

  val resultDF = inputDf.withColumn("parent_tag", explode($"parent_tag"))
    .withColumn("seq", $"parent_tag.seq").withColumn("id", $"parent_tag.id")
    .withColumn("value", $"parent_tag.value").drop("parent_tag")

  resultDF.show(false)
}
