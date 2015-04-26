package interretis.sparktraining;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class WordcountTest {

    /** System under test. */
    private Wordcount wordcount;

    @Before
    public void setUp() {
    }

    @Test
    public void test() throws IOException {

        // given
        final File outputDir = new File("./target/hdfs/wordcount/output");
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }

        final Wordcount wordcount = new Wordcount("src/main/resources/words.txt", "target/hdfs/wordcount/output");

        final SparkConf config = new SparkConf();
        config.setMaster("local");
        config.setAppName("some application");

        final JavaSparkContext context = new JavaSparkContext(config);

        // when
        wordcount.runJob(context);

        // then it doesn't break
    }
}
