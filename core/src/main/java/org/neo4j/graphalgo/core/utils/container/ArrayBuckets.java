package org.neo4j.graphalgo.core.utils.container;

import java.util.Arrays;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * @author mknblch
 */
public class ArrayBuckets implements Buckets {

    private final int[] buckets;

    public ArrayBuckets(int capacity) {
        buckets = new int[capacity];
        Arrays.fill(buckets, -1);

    }

    @Override
    public boolean isEmpty() {
        return IntStream.range(0, buckets.length)
                .parallel()
                .allMatch(b -> b == -1);
    }

    @Override
    public void add(int nodeId, int bucket) {
        buckets[nodeId] = bucket;
    }

    @Override
    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (int bucket : buckets) {
            if (bucket == -1) {
                continue;
            }
            if (bucket < min) {
                min = bucket;
            }
        }
        return min;
    }

    @Override
    public void forEachInBucket(int bucket, IntPredicate consumer) {
        for (int nodeId = 0; nodeId < buckets.length; nodeId++) {
            int tb = buckets[nodeId];
            buckets[nodeId] = -1; // clear bucket
            if (tb == bucket) {
                if (!consumer.test(nodeId)) {
                    return;
                }
            }
        }
    }


}
