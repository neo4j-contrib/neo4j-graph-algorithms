package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayDeque;

import java.util.Arrays;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * @author mknblch
 */
public class Buckets {

    private final int[] buckets;

    public Buckets(int capacity) {
        buckets = new int[capacity];
        Arrays.fill(buckets, -1);
    }

    public boolean isEmpty() {
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != -1) {
                return false;
            }
        }
        return true;
    }

    public void set(int nodeId, int bucket) {
        buckets[nodeId] = bucket;
    }

    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < buckets.length; i++) {
            int bucket = buckets[i];
            if (bucket == -1) {
                continue;
            }
            if (bucket < min) {
                min = bucket;
            }
        }
        return min == Integer.MAX_VALUE ? -1 : min;
    }

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


//    public void forEachInBucket(int bucket, IntPredicate consumer) {
//        IntArrayDeque deque = new IntArrayDeque();
//        for (int nodeId = 0; nodeId < buckets.length; nodeId++) {
//            int tb = buckets[nodeId];
//            if (tb == bucket) {
//                buckets[nodeId] = -1; // clear bucket
//                deque.addLast(nodeId);
//            }
//        }
//        deque.forEach((com.carrotsearch.hppc.predicates.IntPredicate) consumer::test);
//    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int bucket : buckets) {
            builder.append(String.format("[%2d]", bucket));
        }
        return builder.toString();
    }

}
