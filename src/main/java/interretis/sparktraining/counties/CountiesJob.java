package interretis.sparktraining.counties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Collections;
import java.util.List;

public class CountiesJob {

    public static void main(final String... args) {

        final SparkConf config = new SparkConf();
        config.setAppName("Counties");

        final JavaSparkContext context = new JavaSparkContext(config);
    }

    public void run(final JavaRDD<String> input) {

        final JavaRDD<CountyRecord> records = input.map(LINE_TO_RECORD);
        final long countiesCount = records.count();
        System.out.println("Counties count: " + countiesCount);

        final JavaPairRDD<String, Iterable<CountyRecord>> countiesByState = records.groupBy(GRUPPER_BY_STATE);
        final long stateCount = countiesByState.count();
        System.out.println("States count: " + stateCount);

        final JavaRDD<String> states = countiesByState.keys();
        final List<String> statesList = states.collect();
        Collections.sort(statesList);
        System.out.println("States: " + statesList);
    }

    private static final Function<String, CountyRecord> LINE_TO_RECORD = new Function<String, CountyRecord>() {
        @Override
        public CountyRecord call(final String line) throws Exception {
            return CountyRecord.fromLine(line);
        }
    };

    private static final Function<CountyRecord, String> GRUPPER_BY_STATE = new Function<CountyRecord, String>() {
        @Override
        public String call(final CountyRecord record) throws Exception {
            return record.getState();
        }
    };
}
