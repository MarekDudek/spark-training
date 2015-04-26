package interretis.sparktraining;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertThat;

public class WordcountTest {

    /** System under test. */
    private Wordcount wordcount;

    private static List<String> LINES;
    private static List<Tuple2<String, Integer>> TUPLES = asList(
            new Tuple2<String, Integer>("one", 10),
            new Tuple2<String, Integer>("two", 9),
            new Tuple2<String, Integer>("three", 8),
            new Tuple2<String, Integer>("four", 7),
            new Tuple2<String, Integer>("five", 6),
            new Tuple2<String, Integer>("six", 5),
            new Tuple2<String, Integer>("seven", 4),
            new Tuple2<String, Integer>("eight", 3),
            new Tuple2<String, Integer>("nine", 2),
            new Tuple2<String, Integer>("ten", 1)
    );

    private static SparkConf CONFIG;
    private static JavaSparkContext CONTEXT;

    @BeforeClass
    public static void setUpClass() throws IOException {

        CONFIG = new SparkConf();
        CONFIG.setMaster("local");
        CONFIG.setAppName("Wordcount");

        CONTEXT = new JavaSparkContext(CONFIG);

        LINES = Files.readLines(new File("src/main/resources/words.txt"), Charsets.UTF_8);
    }

    @AfterClass
    public static void tearDownClass() {

        CONTEXT.stop();
    }

    @Before
    public void setUp() throws IOException {

        final File outputDir = new File("./target/hdfs/wordcount/output");
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }

    @Test
    public void test() throws IOException {

        // given
        final JavaRDD<String> input = CONTEXT.textFile("src/main/resources/words.txt");

        // when
        wordcount = new Wordcount(input);
        final JavaPairRDD<String, Integer> output = wordcount.runJob();

        // then
        output.saveAsTextFile("target/hdfs/wordcount/output");
    }

    @Test
    public void second_test() throws IOException {

        // given
        final JavaRDD<String> input = CONTEXT.parallelize(LINES);

        // when
        wordcount = new Wordcount(input);
        final JavaPairRDD<String, Integer> output = wordcount.runJob();

        // then
        final JavaPairRDD<String, Integer> expected = CONTEXT.parallelizePairs(TUPLES);
        assertThat(expected, new JavaPairRDDMatcher(output));
    }
}
