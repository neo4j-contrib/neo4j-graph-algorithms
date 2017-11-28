package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
public interface HugeRelationshipPredicate {

    boolean exists(long sourceNodeId, long targetNodeId, Direction direction);

    default boolean exists(long sourceNodeId, long targetNodeId) {
        return exists(sourceNodeId, targetNodeId, Direction.OUTGOING);
    }
}
