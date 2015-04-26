package interretis.sparktraining;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class WordcountTest {

    /** System under test. */
    private Wordcount wordcount;

    private static SparkConf config;
    private static JavaSparkContext context;

    @BeforeClass
    public static void setUpClass() {

        config = new SparkConf();
        config.setMaster("local");
        config.setAppName("Wordcount");

        context = new JavaSparkContext(config);
    }

    @AfterClass
    public static void tearDownClass() {

        context.stop();
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
        final Wordcount wordcount = new Wordcount("src/main/resources/words.txt", "target/hdfs/wordcount/output");

        // when
        wordcount.runJob(context);

        // then it doesn't break
    }

    @Test
    public void second_test() throws IOException {

        // given
        final Wordcount wordcount = new Wordcount("src/main/resources/words.txt", "target/hdfs/wordcount/output");

        // when
        wordcount.runJob(context);

        // then it doesn't break
    }
}
