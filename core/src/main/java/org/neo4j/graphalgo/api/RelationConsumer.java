package org.neo4j.graphalgo.api;

/**
 * @author mknblch
 */
public interface RelationConsumer {

    /**
     * Called for every Edge that matches a given relation-constraint
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @param relationId original relation/edge-id
     */
    void accept(int sourceNodeId, int targetNodeId, long relationId);
}
