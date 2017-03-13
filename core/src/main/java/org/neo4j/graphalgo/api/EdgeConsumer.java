package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 *         added 02.03.2017.
 */
public interface EdgeConsumer {

    void accept(int sourceVertexId, int targetVertexId, long edgeId);
}
