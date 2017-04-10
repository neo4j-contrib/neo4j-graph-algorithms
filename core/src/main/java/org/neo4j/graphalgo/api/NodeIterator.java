package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.function.IntConsumer;

/**
 * Iterate over each graph-nodeId
 *
 * @author mknblch
 */
public interface NodeIterator {
    /**
     * Iterate over each graph-nodeId
     */
    void forEachNode(IntConsumer consumer);

    /**
     * get graph-nodeId iterator
     */
    PrimitiveIntIterator nodeIterator();
}
