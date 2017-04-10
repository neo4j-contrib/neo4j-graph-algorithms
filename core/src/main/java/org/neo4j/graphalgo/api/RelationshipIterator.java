package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface RelationshipIterator extends IncomingRelationshipIterator, OutgoingRelationshipIterator {

    void forEachRelation(int nodeId, Direction direction, RelationshipConsumer consumer);

    Iterator<RelationshipCursor> relationIterator(int nodeId, Direction direction);

    @Override
    default void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        forEachRelation(nodeId, Direction.INCOMING, consumer);
    }

    @Override
    default void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        forEachRelation(nodeId, Direction.OUTGOING, consumer);
    }

    @Override
    default Iterator<RelationshipCursor> incomingIterator(int nodeId) {
        return relationIterator(nodeId, Direction.INCOMING);
    }

    @Override
    default Iterator<RelationshipCursor> outgoingIterator(int nodeId) {
        return relationIterator(nodeId, Direction.OUTGOING);
    }
}
