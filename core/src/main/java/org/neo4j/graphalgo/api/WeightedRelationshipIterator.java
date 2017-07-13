package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * @author mknblch
 */
@Deprecated
public interface WeightedRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer);
}
