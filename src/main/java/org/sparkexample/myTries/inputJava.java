package org.sparkexample.myTries;

/**
 * Created by andrew.addison on 2/17/17.
 */
public class inputJava
{
   /*public static void main(String[] args) {
      long startTime = System.nanoTime();


      String inputFile = args[0];
      String outputFile = args[1];
      String master = "local[*]";

      SparkConf conf = new SparkConf()
                             .setAppName(WordCountTask.class.getName())
                             .setMaster(master);
      JavaSparkContext sc = new JavaSparkContext(conf);
      JavaRDD<String> input = sc.textFile(inputFile);
      //JavaRDD<String> words = inputJava.flatMap( new FlatMapFunction<String, String>() {  public Iterator<String> call(String x) {      return Arrays.asList(x.split(" "));
      //}});
      JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
      JavaPairRDD<String, Integer> counts = words.mapToPair(  new PairFunction<String, String, Integer>(){    public Tuple2<String, Integer> call(String x){      return new Tuple2(x, 1);
      }}).reduceByKey(new Function2<Integer, Integer, Integer>(){        public Integer call(Integer x, Integer y){ return x + y;}});
      counts.saveAsTextFile(outputFile);
      long endTime = System.nanoTime();

long duration = (endTime - startTime);
System.out.println(duration);

   }
*/
}
