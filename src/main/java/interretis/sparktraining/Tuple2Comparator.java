package interretis.sparktraining;

import com.google.common.collect.ComparisonChain;
import scala.Tuple2;

import java.io.Serializable;

public final class Tuple2Comparator<FIRST extends Comparable<FIRST>, SECOND extends Comparable<SECOND>> implements java.util.Comparator<Tuple2<FIRST, SECOND>>, Serializable {

    @Override
    public int compare(final Tuple2<FIRST, SECOND> first, final Tuple2<FIRST, SECOND> second) {
        return ComparisonChain.start()
                .compare(first._1(), second._1())
                .compare(first._2(), second._2())
                .result();
    }
}
