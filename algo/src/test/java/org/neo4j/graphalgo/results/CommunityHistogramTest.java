package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommunityHistogramTest {
    @Test
    public void oneCommunity() {
        final LongLongMap communitySizeMap = new LongLongScatterMap();
        communitySizeMap.addTo(1, 4);

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }

    @Test
    public void multipleCommunities() {
        final LongLongMap communitySizeMap = new LongLongScatterMap();
        communitySizeMap.addTo(1, 4);
        communitySizeMap.addTo(2, 10);
        communitySizeMap.addTo(3, 9);
        communitySizeMap.addTo(4, 8);
        communitySizeMap.addTo(5, 7);

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        assertEquals(10.0, histogram.getValueAtPercentile(100D), 0.01);
        assertEquals(8.0, histogram.getValueAtPercentile(50D), 0.01);
    }
}