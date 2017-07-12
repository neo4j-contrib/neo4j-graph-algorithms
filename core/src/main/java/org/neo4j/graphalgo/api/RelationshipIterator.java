package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public interface RelationshipIterator extends IncomingRelationshipIterator, OutgoingRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer);

    @Override
    default void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.INCOMING, consumer);
    }

    @Override
    default void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.OUTGOING, consumer);
    }
}
