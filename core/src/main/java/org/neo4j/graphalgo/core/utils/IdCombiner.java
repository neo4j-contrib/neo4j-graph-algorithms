package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.RelationshipCursor;

@FunctionalInterface
public interface IdCombiner {
    long apply(int startId, int endId);

    default long apply(RelationshipCursor cursor) {
        return apply(cursor.sourceNodeId, cursor.targetNodeId);
    }
}
