package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * Iterate over each node Id until either
 * all nodes have been consumed or the consumer
 * decides to stop the iteration.
 *
 * @author mknblch
 */
public interface NodeIterator {
    /**
     * Iterate over each nodeId
     */
    void forEachNode(IntPredicate consumer);

    /**
     * get graph-nodeId iterator
     */
    PrimitiveIntIterator nodeIterator();
}
