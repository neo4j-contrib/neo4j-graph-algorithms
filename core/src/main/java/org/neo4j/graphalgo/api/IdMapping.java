package org.neo4j.graphalgo.api;

/**
 *
 *
 * @author mknblch
 */
public interface IdMapping {

    /**
     * Map neo4j nodeId to inner nodeId
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
