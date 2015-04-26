package interretis.sparktraining;

import org.apache.spark.api.java.JavaPairRDD;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

public class JavaPairRDDMatcher<FIRST, SECOND> extends TypeSafeMatcher<JavaPairRDD<FIRST, SECOND>> {

    private static final Tuple2Comparator TUPLE2_COMPARATOR = new Tuple2Comparator();

    private final JavaPairRDD<FIRST, SECOND> actual;

    private int actualSize;
    private int expectedSize;

    private int i;
    private Tuple2<FIRST, SECOND> actualTuple;
    private Tuple2<FIRST, SECOND> expectedTuple;

    public JavaPairRDDMatcher(final JavaPairRDD<FIRST, SECOND> actual) {
        this.actual = actual;
    }

    @Override
    protected boolean matchesSafely(final JavaPairRDD<FIRST, SECOND> expected) {

        final List<Tuple2<FIRST, SECOND>> actualList = actual.collect();
        final List<Tuple2<FIRST, SECOND>> expectedList = expected.collect();

        actualSize = actualList.size();
        expectedSize = expectedList.size();

        if (actualSize != expectedSize) {
            return false;
        }

        Collections.sort(actualList, TUPLE2_COMPARATOR);
        Collections.sort(expectedList, TUPLE2_COMPARATOR);

        for (i = 0; i < actualList.size(); i++) {

            actualTuple = actualList.get(i);
            expectedTuple = expectedList.get(i);

            final int result = TUPLE2_COMPARATOR.compare(actualTuple, expectedTuple);
            if (result != 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void describeTo(final Description description) {

        if (actualSize != expectedSize) {
            description.appendText(format("Counts didn't match, expected was %d and actual %d", expectedSize, actualSize));
            return;
        }

        description.appendText(format("Mismatch at position %d, expected was %s and actual %s", i, expectedTuple, actualTuple));
    }
}
