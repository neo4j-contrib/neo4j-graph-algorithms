package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public interface RelationIterator extends IncomingRelations, OutgoingRelations {

    void forEachRelation(int nodeId, Direction direction, RelationConsumer consumer);

    Iterator<RelationCursor> relationIterator(int nodeId, Direction direction);

    @Override
    default void forEachIncoming(int nodeId, RelationConsumer consumer) {
        forEachRelation(nodeId, Direction.INCOMING, consumer);
    }

    @Override
    default void forEachOutgoing(int nodeId, RelationConsumer consumer) {
        forEachRelation(nodeId, Direction.OUTGOING, consumer);
    }

    @Override
    default Iterator<RelationCursor> incomingIterator(int nodeId) {
        return relationIterator(nodeId, Direction.INCOMING);
    }

    @Override
    default Iterator<RelationCursor> outgoingIterator(int nodeId) {
        return relationIterator(nodeId, Direction.OUTGOING);
    }
}
