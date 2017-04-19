package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface Weights {

    double weightOf(int sourceNodeId, int targetNodeId); // TODO default weight?
}
