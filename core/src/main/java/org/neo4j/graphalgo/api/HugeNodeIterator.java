package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.function.LongPredicate;

/**
 * Iterate over each node Id until either
 * all nodes have been consumed or the consumer
 * decides to stop the iteration.
 *
 * @author mknblch
 */
public interface HugeNodeIterator {
    /**
     * Iterate over each nodeId
     */
    void forEachNode(LongPredicate consumer);

    PrimitiveLongIterator hugeNodeIterator();
}
