package org.neo4j.graphalgo.api;

/**
 * Consumer Interface for weighted edges
 *
 * @author mknblch
 */
public interface WeightedEdgeConsumer {

    /**
     * Called for each edge of the given vertex
     *
     * @param sourceVertexId mapped source vertex id
     * @param targetVertexId mapped target vertex id
     * @param edgeId original edge id
     * @param weight the weight/cost of this edge
     */
    void accept(int sourceVertexId, int targetVertexId, long edgeId, double weight);
}
