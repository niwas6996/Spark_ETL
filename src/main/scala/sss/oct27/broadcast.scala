package sss.oct27

import org.apache.spark.sql.SparkSession

object broadcast extends App{
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  import spark.implicits._

  val blockSize = if (args.length > 2) args(2) else "4096"
  val sc = spark.sparkContext
  val slices = if (args.length > 0) args(0).toInt else 2
  val num = if (args.length > 1) args(1).toInt else 1000000
  val arr1 = (0 until num).toArray
  for (i <- 0 until 3) {
    println(s"Iteration $i")
    println("===========")
    val startTime = System.nanoTime
    val barr1 = sc.broadcast(arr1)
    val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.length)
    // Collect the small RDD so we can print the observed sizes locally.
    observedSizes.collect().foreach(i => println(i))
    println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
  }
  spark.stop()
}

