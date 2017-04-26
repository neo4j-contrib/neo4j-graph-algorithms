package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface NodeWeights {

    double weightOf(int nodeId); // TODO default weight?
}
