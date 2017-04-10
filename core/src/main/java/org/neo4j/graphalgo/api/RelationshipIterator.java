package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface RelationshipIterator extends IncomingRelationshipIterator, OutgoingRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer);

    Iterator<RelationshipCursor> relationshipIterator(int nodeId, Direction direction);

    @Override
    default void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.INCOMING, consumer);
    }

    @Override
    default void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.OUTGOING, consumer);
    }

    @Override
    default Iterator<RelationshipCursor> incomingIterator(int nodeId) {
        return relationshipIterator(nodeId, Direction.INCOMING);
    }

    @Override
    default Iterator<RelationshipCursor> outgoingIterator(int nodeId) {
        return relationshipIterator(nodeId, Direction.OUTGOING);
    }
}
