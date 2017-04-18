package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
@Deprecated
public interface WeightedRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer);

    Iterator<WeightedRelationshipCursor> weightedRelationshipIterator(int nodeId, Direction direction);

    default void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.INCOMING, consumer);
    }

    default Iterator<WeightedRelationshipCursor> incomingWeightedIterator(int nodeId) {
        return weightedRelationshipIterator(nodeId, Direction.INCOMING);
    }

    default void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.OUTGOING, consumer);
    }

    default Iterator<WeightedRelationshipCursor> outgoingWeightedIterator(int nodeId) {
        return weightedRelationshipIterator(nodeId, Direction.OUTGOING);
    }
}
