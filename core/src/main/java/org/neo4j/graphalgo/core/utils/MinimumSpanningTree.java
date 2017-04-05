package org.neo4j.graphalgo.core.utils;


import java.util.Arrays;

/**
 * @author mknblch
 */
public class MinimumSpanningTree {

    public static final int MULTIPLICAND = 100_000;

    private final IntTupleList tuples;
    private final IntMaxPriorityQueue queue;

    public MinimumSpanningTree() {
        this(14); // TODO
    }

    public MinimumSpanningTree(int initialCapacity) {
        tuples = new IntTupleList(3, initialCapacity);
        queue = new IntMaxPriorityQueue(initialCapacity);
    }

    public MinimumSpanningTree(IntTupleList tuples) {
        this.tuples = tuples;
        queue = null;
    }

    public void add(int sourceNodeId, int targetNodeId, double weight) {
        queue.add(tuples.add(sourceNodeId, targetNodeId, (int) (weight * MULTIPLICAND)), weight);
    }

    public int size() {
        return tuples.size();
    }

    public MinimumSpanningTree[] split(int k) {
        k--;
        int[] offsets = new int[k];
        for (int i = 0; i < k; i++) {
            offsets[i] = queue.pop();
        }
        Arrays.sort(offsets);
        final MinimumSpanningTree[] trees = new MinimumSpanningTree[k];
        int head = 0;
        for (int i = 0; i <= k; i++) {
            trees[i] = new MinimumSpanningTree(tuples.slice(head, offsets[i]));
            head = offsets[i];
        }
        return trees;
    }

    @Override
    public String toString() {
        return tuples.toString();
    }
}
