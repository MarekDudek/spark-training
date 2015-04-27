package interretis.sparktraining.counties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class CountiesJobTest {

    /** System under test. */
    private CountiesJob job;

    private static SparkConf CONFIG;
    private static JavaSparkContext CONTEXT;

    @BeforeClass
    public static void setUpClass() throws IOException {

        CONFIG = new SparkConf();
        CONFIG.setMaster("local");
        CONFIG.setAppName("Counties");

        CONTEXT = new JavaSparkContext(CONFIG);
    }

    @AfterClass
    public static void tearDownClass() {

        CONTEXT.stop();
    }

    @Test
    public void test() {

        // given
        final JavaRDD<String> input = CONTEXT.textFile("src/main/resources/counties-headless.csv");

        // when
        job = new CountiesJob();
        job.run(input);

    }
}
