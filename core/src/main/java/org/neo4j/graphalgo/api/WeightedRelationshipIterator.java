package org.neo4j.graphalgo.api;

import org.neo4j.graphdb.Direction;

/**
 * Outdated in favor of org.neo4j.graphalgo.api.RelationshipWeights
 *
 * @author mknblch
 */
@Deprecated
public interface WeightedRelationshipIterator {

    void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer);
}
