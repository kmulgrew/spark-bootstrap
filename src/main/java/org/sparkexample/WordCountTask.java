package org.sparkexample;

public class WordCountTask {
   /*private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

   public static void main(String[] args) {
      checkArgument(args.length >= 1, "Please provide the path of inputJava file as first parameter.");
      new WordCountTask().run(args[0]);
   }

   public void run(String inputFilePath) {
      String master = "local[*]";

      SparkConf conf = new SparkConf()
                             .setAppName(WordCountTask.class.getName())
                             .setMaster(master);
      JavaSparkContext context = new JavaSparkContext(conf);

      context.textFile(inputFilePath)
             .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
             .mapToPair(word -> new Tuple2<>(word, 1))
             .reduceByKey((a, b) -> a + b)
             .foreach(result -> LOGGER.info(
                   String.format("Word [%s] count [%d].", result._1(), result._2)));

   }*/
}
