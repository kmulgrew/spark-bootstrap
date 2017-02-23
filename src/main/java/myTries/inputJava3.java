package myTries;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.sparkexample.WordCountTask;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
/**
 * Created by andrew.addison on 2/23/17.
 */
public class inputJava3
{
   public static void main(String[] args) throws IOException
   {
      long startTime = System.nanoTime();

      String[] inputFolderArray = new String[args.length - 3];
      int folderArrayIndex = 0;
      for (int i = 2; i < args.length - 1; i++)
      {
         //System.out.println(args[i]);
         inputFolderArray[folderArrayIndex++] = args[i];
      }

      String regex = args[args.length - 1]; //"/[wkd]ing";//a
      //System.out.println(regex);
      String master = "local[*]";

      SparkConf conf = new SparkConf().setAppName(WordCountTask.class.getName()).setMaster(master);
      final String user = args[0];
      final String keyPath = args[1];

      UserGroupInformation.loginUserFromKeytab(user, keyPath);
      System.out.println(UserGroupInformation.isLoginKeytabBased() );
      JavaSparkContext sc = new JavaSparkContext(conf);
      JavaRDD<String> input = sc.emptyRDD();
      for (int i = 0; i < inputFolderArray.length; i++)
      {
         JavaRDD<String> input2 = sc.textFile(inputFolderArray[i] + regex); //"/[wkd]ing");
         input = input.union(input2);
      }
      JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
      words = words.repartition(100);
      JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>()
      {
         public Tuple2<String, Integer> call(String x)
         {
            return new Tuple2(x, 1);
         }
      }).reduceByKey(new Function2<Integer, Integer, Integer>()
      {
         public Integer call(Integer x, Integer y)
         {
            return x + y;
         }
      });
      counts.saveAsTextFile("output-java-3");
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      System.out.println(duration);
   }
}