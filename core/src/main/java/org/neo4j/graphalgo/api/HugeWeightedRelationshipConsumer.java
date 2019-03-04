package org.neo4j.graphalgo.api;

public interface HugeWeightedRelationshipConsumer {
    /**
     * Called for every edge that matches a given relation-constraint
     *
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @param weight relationship weight
     * @return {@code true} if the iteration shall continue, otherwise {@code false}.
     */
    boolean accept(
            long sourceNodeId,
            long targetNodeId, double weight);

}



