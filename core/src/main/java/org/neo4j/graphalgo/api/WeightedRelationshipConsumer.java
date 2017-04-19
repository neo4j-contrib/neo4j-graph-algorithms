package org.neo4j.graphalgo.api;

/**
 * Consumer Interface for weighted edges
 *
 * @author mknblch
 */
@Deprecated
public interface WeightedRelationshipConsumer {

    /**
     * Called for each edge of the given node
     *
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @param relationId deprecated
     * @param weight the weight/cost of this edge
     */
    boolean accept(int sourceNodeId, int targetNodeId, @Deprecated long relationId, double weight);
}
