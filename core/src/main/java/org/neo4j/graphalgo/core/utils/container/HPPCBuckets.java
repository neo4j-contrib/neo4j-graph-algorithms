package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class HPPCBuckets implements Buckets {

    public interface BucketConsumer {

        boolean consume(int bucket, int node);
    }
    private final IntObjectMap<IntArrayDeque> buckets;

    public HPPCBuckets() {
        buckets = new IntObjectHashMap<>();
    }

    @Override
    public boolean isEmpty() {
        for (IntObjectCursor<IntArrayDeque> bucket : buckets) {
            if (!bucket.value.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void add(int nodeId, int bucket) {
        if (!buckets.containsKey(bucket)) {
            IntArrayDeque list = new IntArrayDeque(1);
            list.addLast(nodeId);
            buckets.put(bucket, list);
        }
    }


    @Override
    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (IntObjectCursor<IntArrayDeque> bucket : buckets) {
            IntArrayDeque deque = bucket.value;
            if (deque.isEmpty()) {
                continue;
            }
            if (bucket.key < min) {
                min = bucket.key;
            }
        }

        return min;
    }

    @Override
    public void forEachInBucket(int bucket, IntPredicate consumer) {

    }


}
