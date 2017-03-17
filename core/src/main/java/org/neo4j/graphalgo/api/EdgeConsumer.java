package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface EdgeConsumer {

    /**
     * Called for every Edge that matches a given relation-constraint
     * @param sourceVertexId mapped source node id
     * @param targetVertexId mapped target node id
     * @param edgeId original relation/edge-id
     */
    void accept(int sourceVertexId, int targetVertexId, long edgeId);
}
