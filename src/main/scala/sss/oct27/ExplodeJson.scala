package sss.oct27

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object ExplodeJson extends App {

  val spark = SparkSession.builder().master("local").appName("ExplodeJson").getOrCreate()
  val df =  spark.read.option("multiLine",true).option("inferSchema",true)
    .json("C:\\Users\\Niwas\\IdeaProjects\\untitled\\mocked_test_data.json")
  df.show()
  import spark.implicits._
  val df1 = df.select(explode($"stackoverflow") as "stackoverflow_tags")

  df1.select(
    $"stackoverflow_tags.tag.id" as "id",
          $"stackoverflow_tags.tag.author" as "author",
          $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"

  ).show(false)
}