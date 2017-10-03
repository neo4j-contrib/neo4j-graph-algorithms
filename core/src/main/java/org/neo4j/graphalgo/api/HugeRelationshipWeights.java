package org.neo4j.graphalgo.api;

/**
 * Getter for weight property values at relationships
 *
 * @author mknblch
 */
public interface HugeRelationshipWeights {

    /**
     * get weight between source and target node id
     *
     * @param sourceNodeId source node
     * @param targetNodeId target node
     * @return the weight
     */
    double weightOf(long sourceNodeId, long targetNodeId);

    /**
     * get weight between source and target node id or the default weight
     *
     * @param sourceNodeId  source node
     * @param targetNodeId  target node
     * @param defaultWeight default weight
     * @return the weight
     */
    double weightOf(long sourceNodeId, long targetNodeId, double defaultWeight);
}
