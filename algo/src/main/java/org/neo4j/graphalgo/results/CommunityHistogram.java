package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;

public class CommunityHistogram {

    public static Histogram buildFrom(LongLongMap communitySizeMap) {
        final Histogram histogram = new Histogram(2);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        return histogram;
    }
}
