package myTries

import org.apache.spark.sql.SparkSession

object inputScala {
  def main(args: Array[String]) {
     val t0 = System.nanoTime()



    val spark = SparkSession
      .builder
      .appName("inputScala").master("local")
      .getOrCreate()
    val file = args(0)
    val input = spark.sparkContext.textFile(file)
    val output = args(1)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile(output)
     val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
  }
}
