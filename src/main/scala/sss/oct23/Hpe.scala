package com.sand.spark.sql.hpe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, count, explode, rank, split}

object HPEJsonParserTestDriver {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val rawdf = spark.read.option("multiLine", true).json("C:\\Users\\Niwas\\Downloads\\mocked_test_data (1).json")
      .withColumnRenamed("alphanumeric", "country")

    val rankWiseExplodedf = rawdf.withColumn("rank", explode(split($"rankings", ",")))


    val personeWiseHackathonCountdf = rankWiseExplodedf.groupBy("name").agg(count("rank").as("hack_count"))
    //    personeWiseHackathonCountdf.show()

    val hackHighLowRankdf = personeWiseHackathonCountdf.withColumn("hack_count_high_rank", rank().over(Window.orderBy($"hack_count".desc)))
      .withColumn("hack_count_low_rank", rank().over(Window.orderBy($"hack_count".asc))).cache()


    val max_hact_participants = hackHighLowRankdf.filter("hack_count_high_rank == 1")
    val min_hact_participants = hackHighLowRankdf.filter("hack_count_low_rank == 1")

    max_hact_participants.show()
    min_hact_participants.show()


    val rank1OnlyParticipatnsdf = rankWiseExplodedf.filter("rank == 1")
    val rank1PersoneWiseHackathonCountdf = rank1OnlyParticipatnsdf.groupBy("name").agg(count("rank").as("rank1_hack_count"))
    //    personeWiseHackathonCountdf.show()

    val rank1Hightdf = rank1PersoneWiseHackathonCountdf.withColumn("rank1_hack_count_high_rank", rank().over(Window.orderBy($"rank1_hack_count".desc))).filter("rank1_hack_count == 1")
    rank1Hightdf.show()


    val countryWiseCountdf = rawdf.groupBy("country").agg(count("name").as("per_country_count"))
    //    personeWiseHackathonCountdf.show()

    val topPartcipantsCOuntry = countryWiseCountdf.withColumn("top_country_rank", rank().over(Window.orderBy($"per_country_count".desc)))
      .filter("top_country_rank == 1 ")

    topPartcipantsCOuntry.show()


    val avgDf = rankWiseExplodedf.groupBy("country", "name").agg(avg("rank").as("avg_rank"))
    //    personeWiseHackathonCountdf.show()

    val ass4df = avgDf.withColumn("top_avg_rank", rank().over(Window.orderBy($"avg_rank".desc)))
    ass4df.show()
  }
}
