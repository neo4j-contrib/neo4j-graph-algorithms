package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface RelationshipWeights {

    double weightOf(int sourceNodeId, int targetNodeId); // TODO default weight?
}
