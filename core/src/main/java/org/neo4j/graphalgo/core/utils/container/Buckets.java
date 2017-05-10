package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntArrayList;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class Buckets {

    private final int[] buckets;

    public Buckets(int capacity) {
        this.buckets = new int[capacity];
        Arrays.fill(buckets, -1);
    }

    public boolean isEmpty() {
        return Arrays.stream(buckets)
                .allMatch(b -> -1 == b);
    }

    public void set(int bucket, int element) {
        System.out.println("push: " + element + " = " + bucket);
        buckets[element] = bucket;
    }

    public IntArrayDeque get(int bucket) {
        IntArrayDeque list = new IntArrayDeque();
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] == bucket) {
                list.addLast(i);
                buckets[i] = -1;
            }
        }
        return list;
    }

    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != -1 && buckets[i] < min) {
                min = buckets[i];
            }
        }
        return min;
    }
}
