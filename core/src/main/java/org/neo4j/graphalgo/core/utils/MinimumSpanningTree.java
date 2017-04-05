package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.core.utils.IntTupleList;

/**
 * @author mknblch
 */
public class MinimumSpanningTree {

    public static final int MULTIPLICAND = 100_000;

    private final IntTupleList tuples;
    private final IntMinPriorityQueue queue;

    public MinimumSpanningTree(int initialCapacity) {
        tuples = new IntTupleList(3, initialCapacity);
        queue = new IntMinPriorityQueue(initialCapacity);
    }

    public void add(int sourceNodeId, int targetNodeId, double weight) {
        queue.add(tuples.add(sourceNodeId, targetNodeId, (int) (weight * MULTIPLICAND)), weight);
    }

    public

}
