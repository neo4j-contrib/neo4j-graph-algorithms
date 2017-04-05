package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.WeightedRelationCursor;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author mknblch
 */
public class MinimumSpanningTree implements Iterable<WeightedRelationCursor> {

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
        final int[] offsets = new int[k];
        final MinimumSpanningTree[] trees = new MinimumSpanningTree[k];
        k--;
        for (int i = 0; i <= k; i++) {
            offsets[i] = queue.pop();
        }
        Arrays.sort(offsets);
        int head = 0;
        for (int i = 0; i <= k; i++) {
            trees[i] = new MinimumSpanningTree(tuples.slice(head, offsets[i]));
            head = offsets[i];
        }
        trees[k - 1] = new MinimumSpanningTree(tuples.slice(head, tuples.size()));
        return trees;
    }

    @Override
    public String toString() {
        return tuples.toString();
    }

    @Override
    public Iterator<WeightedRelationCursor> iterator() {
        return new ItAdapter(tuples.iterator());
    }

    private static class ItAdapter implements Iterator<WeightedRelationCursor> {

        private final Iterator<int[]> iterator;
        private final WeightedRelationCursor cursor = new WeightedRelationCursor();

        private ItAdapter(Iterator<int[]> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public WeightedRelationCursor next() {
            int[] tuple = iterator.next();
            cursor.sourceNodeId = tuple[0];
            cursor.targetNodeId = tuple[1];
            cursor.weight = (double) tuple[2] / MULTIPLICAND;
            return cursor;
        }
    }

}
