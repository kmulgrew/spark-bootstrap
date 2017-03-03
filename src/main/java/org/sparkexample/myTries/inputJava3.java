package org.sparkexample.myTries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.sparkexample.WordCountTask;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
/**
 * Created by andrew.addison on 2/23/17.
 */

//arg 0 user
   //arg 1 keytab
   //arg 2 fs.defaultDS
   //arg 3 -> (args.length-1) folders to traverse
   //arg args.length-1 regex for file names to match
public class inputJava3
{
   public static void main(String[] args) throws IOException
   {
      long startTime = System.nanoTime();

      System.out.println(Arrays.asList(args).toString());

      String[] inputFolderArray = new String[1];
      inputFolderArray = new String[args.length-1];

      for(int i=1; i<args.length-1; i++) {
         inputFolderArray[i-1] = args[i];
      }


      String regex = args[0]; //"/[wkd]ing";//a
      //System.out.println(regex);

      SparkConf conf = new SparkConf().setAppName(WordCountTask.class.getName());
      //conf.set("hadoop.security.authentication", "kerberos");


      //FileSystem fs = FileSystem.get(conf2);
      //String user2 = Utils.getCurrentUserName();

      //FileSystem f = HdfsConnection.getConnection("carolus_K@KRAKENNYBETA.CIS.HADOOP.INT.THOMSONREUTERS.COM", "/opt/Kraken/conf/keytab/kraken.keytab");

     // conf.set(user, keyPath);
      //System.out.println(UserGroupInformation.getLoginUser().toString());
      //System.out.println(UserGroupInformation.isLoginKeytabBased() );
      //System.out.println(UserGroupInformation.getLoginUser().isFromKeytab());
      //System.out.println(UserGroupInformation.getCurrentUser());
      //System.out.println(UserGroupInformation.isSecurityEnabled());

      JavaSparkContext sc = new JavaSparkContext(conf);

      JavaRDD<String> input = sc.emptyRDD();
      for (int i = 0; i < inputFolderArray.length; i++)
      {
         JavaRDD<String> input2 = sc.textFile(inputFolderArray[i] + regex); //"/[wkd]ing");
         input = input.union(input2);
      }

      //JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
      JavaRDD<String> words = input.flatMap(  new FlatMapFunction<String, String>() {    public Iterable<String> call(String x) {      return Arrays.asList(x.split(" "));    }});

      //words = words.repartition(100);
      /*JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>()
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
      });*/
      JavaPairRDD<String, Integer> counts = words.mapToPair(  new PairFunction<String, String, Integer>(){    public Tuple2<String, Integer> call(String x){      return new Tuple2(x, 1);    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){        public Integer call(Integer x, Integer y){ return x + y;}});

      counts.saveAsTextFile("output-java-3");
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      System.out.println(duration);
   }
}