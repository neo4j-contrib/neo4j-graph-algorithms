package org.neo4j.graphalgo.core.utils.container;

import org.apache.lucene.util.ArrayUtil;

import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public class Forest {

    private int[][] forest;

    public Forest(int buckets) {
        forest = new int[buckets][1];
    }

    public Forest append(int bucket, int value) {
        int index = ++forest[bucket][0];
        forest[bucket] = ArrayUtil.grow(forest[bucket], index + 1);
        forest[bucket][index] = value;
        return this;
    }

    public int size(int bucket) {
        return forest[bucket][0];
    }

    public void forEach(int bucket, IntPredicate consumer) {
        for (int i = forest[bucket][0]; i > 0; i--) {
            if (!consumer.test(forest[bucket][i])) {
                return;
            }
        }
    }
}
