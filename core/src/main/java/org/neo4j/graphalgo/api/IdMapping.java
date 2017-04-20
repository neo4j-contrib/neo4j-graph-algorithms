package org.neo4j.graphalgo.api;

/**
 * bidirectional mapping between long neo4j-nodeId and
 * temporary int graph-nodeId.
 *
 * @author mknblch
 */
public interface IdMapping {

    /**
     * defines the lower bound of mapped node ids
     * TODO: function?
     */
    int START_NODE_ID = 0;

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
