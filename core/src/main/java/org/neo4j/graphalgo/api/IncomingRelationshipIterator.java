package org.neo4j.graphalgo.api;

import java.util.Iterator;

/**
 * Iterator for incoming relations based on nodeId.
 *
 * @author mknblch
 */
public interface IncomingRelationshipIterator {

    /**
     * Iterates over each relationship in the nodeSet
     * or until the consumer stops the iteration.
     *
     * @param nodeId node id
     * @param consumer a relationship consumer
     */
    void forEachIncoming(int nodeId, RelationshipConsumer consumer);

    Iterator<RelationshipCursor> incomingIterator(int nodeId);
}
