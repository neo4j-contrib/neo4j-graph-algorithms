package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.predicates.IntPredicate;

import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class Buckets {

    public interface BucketConsumer {

        boolean consume(int bucket, int node);
    }
    private final IntObjectMap<IntArrayDeque> buckets;

    public Buckets() {
        buckets = new IntObjectHashMap<>();
    }

    public boolean isEmpty() {
        for (IntObjectCursor<IntArrayDeque> bucket : buckets) {
            if (!bucket.value.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public void set(int bucket, int nodeId) {
        if (!buckets.containsKey(bucket)) {
            IntArrayDeque list = new IntArrayDeque(1);
            list.addLast(nodeId);
            buckets.put(bucket, list);
        } else {
            get(bucket).addLast(nodeId);
        }
    }

    public IntArrayDeque get(int bucket) {
        return buckets.get(bucket);
    }

    public boolean remove(int bucket, int nodeId) {
        IntArrayDeque deque = buckets.get(bucket);
        if (null == deque) {
            return false;
        }
        if (-1 == deque.removeLast(nodeId)) {
            return false;
        }
        return true;
    }

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


}
