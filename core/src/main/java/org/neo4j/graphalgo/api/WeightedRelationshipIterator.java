package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface WeightedRelationshipIterator extends WeightedIncomingRelationshipIterator, WeightedOutgoingRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer);

    Iterator<WeightedRelationshipCursor> weightedRelationshipIterator(int nodeId, Direction direction);

    @Override
    default void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.INCOMING, consumer);
    }

    @Override
    default Iterator<WeightedRelationshipCursor> incomingWeightedIterator(int nodeId) {
        return weightedRelationshipIterator(nodeId, Direction.INCOMING);
    }

    @Override
    default void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.OUTGOING, consumer);
    }

    @Override
    default Iterator<WeightedRelationshipCursor> outgoingWeightedIterator(int nodeId) {
        return weightedRelationshipIterator(nodeId, Direction.OUTGOING);
    }
}
