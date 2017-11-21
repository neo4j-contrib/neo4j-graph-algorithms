package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public interface RelationshipPredicate {

    boolean exists(int sourceNodeId, int targetNodeId, Direction direction);

    default boolean exists(int sourceNodeId, int targetNodeId) {
        return exists(sourceNodeId, targetNodeId, Direction.OUTGOING);
    }
}
