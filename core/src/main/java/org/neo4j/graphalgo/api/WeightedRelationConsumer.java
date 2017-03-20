package org.neo4j.graphalgo.api;

/**
 * Consumer Interface for weighted edges
 *
 * @author mknblch
 */
public interface WeightedRelationConsumer {

    /**
     * Called for each edge of the given node
     *
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @param relationId relation id
     * @param weight the weight/cost of this edge
     */
    void accept(int sourceNodeId, int targetNodeId, long relationId, double weight);
}
