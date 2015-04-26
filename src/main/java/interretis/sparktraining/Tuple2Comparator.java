package interretis.sparktraining;

import com.google.common.collect.ComparisonChain;
import scala.Tuple2;

public class Tuple2Comparator implements java.util.Comparator<Tuple2<String, Integer>> {

    @Override
    public int compare(final Tuple2<String, Integer> first, final Tuple2<String, Integer> second) {
        return ComparisonChain.start()
                .compare(first._1(), second._1())
                .compare(first._2(), second._2())
                .result();
    }
}
