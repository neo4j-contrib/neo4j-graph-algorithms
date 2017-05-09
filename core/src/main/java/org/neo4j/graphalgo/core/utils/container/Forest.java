package org.neo4j.graphalgo.core.utils.container;

import org.apache.lucene.util.ArrayUtil;

import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class Forest {

    private int[][] forest;

    public Forest() {
    }

    public Forest(int buckets) {
        forest = new int[buckets][1];
    }

    public Forest push(int bucket, int value) {
        forest = ArrayUtil.grow(forest, bucket);
        if (null == forest[bucket]) {
            forest[bucket] = new int[1];
        }
        int index = ++forest[bucket][0];
        forest[bucket] = ArrayUtil.grow(forest[bucket], index + 1);
        forest[bucket][index] = value;
        return this;
    }

    public int pop(int bucket) {
        return forest[bucket][forest[bucket][0]-- + 1];
    }

    public int size(int bucket) {
        return forest[bucket][0];
    }

    public int size() {
        int sum = 0;
        for (int i = forest.length - 1; i >= 0; i--) {
            sum += forest[i][0];
        }
        return sum;
    }

    public void forEach(int bucket, IntPredicate consumer) {
        for (int i = forest[bucket].length - 1; i > 0; i--) {
            final int value = forest[bucket][i];
            if (value == -1) {
                continue;
            }
            if (!consumer.test(value)) {
                return;
            }
        }
    }
}
