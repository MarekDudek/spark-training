package interretis.sparktraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Wordcount {

    public static final FlatMapFunction<String, String> SPLIT_TO_WORDS = new FlatMapFunction<String, String>() {
        @Override
        public Iterable<String> call(final String string) throws Exception {
            final String[] words = string.split("\\W+");
            return Arrays.asList(words);
        }
    };
    public static final PairFunction<String, String, Integer> WORD_TO_PAIR = new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(final String word) throws Exception {
            return new Tuple2<>(word, Integer.valueOf(1));
        }
    };
    public static final Function2<Integer, Integer, Integer> COUNT_ADDER = new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(final Integer accumulator, final Integer count) throws Exception {
            return Integer.valueOf(accumulator.intValue() + count.intValue());
        }
    };

    private final String inputPath;
    private final String outputPath;

    public Wordcount(final String inputPath, final String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static void main(final String... args) {

        if (args.length != 2) {
            throw new IllegalArgumentException("I need two arguments and I got " + args.length);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        final Wordcount wordcount = new Wordcount(inputPath, outputPath);

        final SparkConf config = new SparkConf();
        config.setAppName("Wordcount with Spark in Java");

        final JavaSparkContext context = new JavaSparkContext(config);

        wordcount.runJob(context);
    }

    public void runJob(final JavaSparkContext context) {

        final JavaRDD<String> input = context.textFile(inputPath);

        final JavaRDD<String> words = input.flatMap(SPLIT_TO_WORDS);
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(WORD_TO_PAIR);
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey(COUNT_ADDER);

        counts.saveAsTextFile(outputPath);
    }
}
