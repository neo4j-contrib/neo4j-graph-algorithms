package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 *         added 02.03.2017.
 */
public interface WeightedEdgeConsumer {

    void accept(int sourceVertexId, int targetVertexId, long edgeId, double weight);
}
