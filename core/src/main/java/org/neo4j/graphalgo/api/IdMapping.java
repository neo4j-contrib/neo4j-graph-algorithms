package org.neo4j.graphalgo.api;

/**
 * bidirectional mapping between long neo4j-nodeId and
 * temporary int graph-nodeId.
 *
 * @author mknblch
 */
public interface IdMapping {

    /**
     * Map neo4j nodeId to inner nodeId
     * TODO rename?
     */
    int toMappedNodeId(long nodeId);

    /**
     * Map inner nodeId back to original nodeId
     */
    long toOriginalNodeId(int nodeId);

    /**
     * count of nodes
     */
    int nodeCount();
}
