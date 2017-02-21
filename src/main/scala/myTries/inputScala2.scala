package myTries


import org.apache.spark.sql.SparkSession

object inputScala2 {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()

  //println(args(0))

    val spark = SparkSession
      .builder
      .appName("inputScala").master("local")
      .getOrCreate()
    val len = args.size-1
   // val inputFolderarray: Array[java.lang.String] = Array()
    val inputFolderarray = new Array[String](len)

    for(i<-0 until args.size-1) {
      inputFolderarray(i) = args(i)
      //println(args(i))
    }

    val regex = args(args.size-1)
    var input = spark.sparkContext.emptyRDD[String]//textFile(file)
    for(i<-0 until inputFolderarray.size) {
      input = input.union(spark.sparkContext.textFile(inputFolderarray(i) + regex))
    }


    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile("output-scala-2")
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
  }
}