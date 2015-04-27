package interretis.sparktraining.counties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CountiesJob {

    public static void main(final String... args) {

        final SparkConf config = new SparkConf();
        config.setAppName("Counties");

        final JavaSparkContext context = new JavaSparkContext(config);
    }

    public void run(final JavaRDD<String> input) {

        final JavaRDD<CountyRecord> records = input.map(LINE_TO_RECORD);

        final long count = records.count();
        System.out.println(count);
    }

    private static final Function<String, CountyRecord> LINE_TO_RECORD = new Function<String, CountyRecord>() {
        @Override
        public CountyRecord call(final String line) throws Exception {
            return CountyRecord.fromLine(line);
        }
    };
}
