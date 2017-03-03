package org.sparkexample.myTries

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object inputScala3 {
  def main(args: Array[String]) {
     val t0 = System.nanoTime()
    val conf = new SparkConf().setAppName("wordCount")
  val sc = new SparkContext(conf)
  // Load our input data.val
   //val inputFile = args(0)
    //val regex = args(1)

    val len = args.size-1
    // val inputFolderarray: Array[java.lang.String] = Array()
    val inputFolderarray = new Array[String](len)

    for(i<-1 until args.size-1) {
      inputFolderarray(i-1) = args(i)
      //println(args(i))
    }
    val regex = args(0)
    val input : RDD[String] = returnInput(sc,inputFolderarray, regex)

    //var input = sc.emptyRDD[String]//textFile(file)
    /*for(i<-0 until inputFolderarray.size) {
      input = input.union(sc.textFile(inputFolderarray(i) + regex))
    }*/
  //val input = returnInput(sc, inputFile, regex)
    //val input = sc.textFile(inputFile + regex)
  // Split it up into words.

  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
  counts.saveAsTextFile("output-scala-3")
val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
}
  def returnInput(sc: SparkContext, inputFolderArray: Array[String], regex: String): RDD[String] = {
    val x = for {
      i <- inputFolderArray
    } yield sc.textFile(i + regex)
    x.fold(sc.parallelize(Seq[String]()))(_ union _)
  }
}
