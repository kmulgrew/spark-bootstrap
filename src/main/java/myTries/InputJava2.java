package myTries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.sparkexample.WordCountTask;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by andrew.addison on 2/21/17.
 */
public class InputJava2
{
   public static void main(String[] args) {
      long startTime = System.nanoTime();


      String[] inputFolderArray  = new String[args.length-1];

      for(int i = 0; i<args.length-1;i++) {
         System.out.println(args[i]);
         inputFolderArray[i] = args[i];
      }

      String regex = args[args.length-1]; //"/[wkd]ing";//a
      //System.out.println(regex);
      String master = "local[*]";

      SparkConf conf = new SparkConf()
                             .setAppName(WordCountTask.class.getName())
                             .setMaster(master);
      JavaSparkContext sc = new JavaSparkContext(conf);
      JavaRDD<String> input = sc.emptyRDD();
      for(int i = 0; i<inputFolderArray.length; i++) {
         JavaRDD<String> input2 = sc.textFile(inputFolderArray[i] + regex); //"/[wkd]ing");
         input = input.union(input2);
      }
      JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
      JavaPairRDD<String, Integer>
      counts = words.mapToPair(new PairFunction<String, String, Integer>(){    public Tuple2<String, Integer> call(String x){      return new Tuple2(x, 1);
      }}).reduceByKey(new Function2<Integer, Integer, Integer>(){        public Integer call(Integer x, Integer y){ return x + y;}});
      counts.saveAsTextFile("output-java-2");
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      System.out.println(duration);

   }

}
