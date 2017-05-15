package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayDeque;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * container for assigning nodeIds to arbitrary buckets
 * @author mknblch
 */
public class Buckets {

    private final AtomicIntegerArray buckets;
    private final int capacity;

    public Buckets(int capacity) {
        buckets = new AtomicIntegerArray(capacity);
        this.capacity = capacity;
        reset();
    }

    /**
     * reset all buckets
     */
    public void reset() {
        for (int i = 0; i < capacity; i++) {
            buckets.set(i, -1);
        }
    }

    /**
     * check if any bucket has nodes left
     * @return if the no nodes left, false otherwise
     */
    public boolean isEmpty() {
        for (int i = 0; i < capacity; i++) {
            if (buckets.get(i) != -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * assign bucket to nodeId
     * @param nodeId the node id
     * @param bucket the bucket index
     */
    public void set(int nodeId, int bucket) {
        buckets.set(nodeId, bucket);
    }

    /**
     * find smallest non empty bucket index
     * @return the index
     */
    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < capacity; i++) {
            int bucket = buckets.get(i);
            if (bucket == -1) {
                continue;
            }
            if (bucket < min) {
                min = bucket;
            }
        }
        return min;
    }

    /**
     * iterate over each node in bucket and call consumer.
     * also clears the bucket before executing the consumer.
     * @param bucket the bucket index
     * @param consumer the nodeConsumer
     */
    public void forEachInBucket(int bucket, IntPredicate consumer) {
        for (int nodeId = 0; nodeId < capacity; nodeId++) {
            int tb = buckets.get(nodeId);
            if (tb == bucket) {
                buckets.set(nodeId, -1); // clear bucket
                if (!consumer.test(nodeId)) {
                    return;
                }
            }
        }
    }
}
