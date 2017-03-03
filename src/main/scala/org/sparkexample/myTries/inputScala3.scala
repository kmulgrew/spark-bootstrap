package org.sparkexample.myTries

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object inputScala3 {
  def main(args: Array[String]) {
     val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("wordCount")
  val sc = new SparkContext(conf)
  // Load our input data.val
   val inputFile = args(0)
    val regex = args(1)


  //val input = returnInput(sc, inputFile, regex)
    val input = sc.textFile(inputFile + regex)
  // Split it up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
  counts.saveAsTextFile("output-scala-3")
val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
}
  /*def returnInput(sc: SparkContext, inputFile: String, regex: String): RDD[String] = {
    for(i<-0 until inputFile.size) {
      var input: RDD[String] = input.union(sc.textFile(inputFile(i) + regex))
      if(i==inputFile.size-1) return input
    }
  }*/
}
