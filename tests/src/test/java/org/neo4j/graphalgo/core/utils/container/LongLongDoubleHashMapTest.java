package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.helpers.collection.Pair;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public final class LongLongDoubleHashMapTest extends RandomizedTest {

    @Test
    public void testMap() throws Exception {
        int size = between(200, 500);

        LongLongDoubleHashMap map = new LongLongDoubleHashMap(size);
        Map<Pair<Long, Long>, Double> map2 = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            long k1 = between(0, size - 1);
            long k2 = between(0, size - 1);
            double v = biasedDoubleBetween(0, 1.0);

            Double prev = map2.put(Pair.of(k1, k2), v);
            double expected = 0;
            if (prev != null) {
                expected = prev;
            }
            double actual = map.put(k1, k2, v);
            assertEquals(
                    String.format("put( %d, %d, %s)", k1, k2, v),
                    expected,
                    actual,
                    0.01);
        }

        for (Map.Entry<Pair<Long, Long>, Double> entry : map2.entrySet()) {
            long k1 = entry.getKey().first();
            long k2 = entry.getKey().other();
            double expected = entry.getValue();
            double actual = map.getOrDefault(k1, k2, 42.0);
            assertEquals(
                    String.format("get(%d, %d)", k1, k2),
                    expected,
                    actual,
                    0.01
            );
        }
    }
}
