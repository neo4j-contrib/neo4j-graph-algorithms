package org.neo4j.graphalgo.api;

/**
 * bidirectional mapping between long neo4j-nodeId and
 * temporary int graph-nodeId.
 *
 * @author mknblch
 */
public interface HugeIdMapping {

    /**
     * defines the lower bound of mapped node ids
     * TODO: function?
     */
    long START_NODE_ID = 0;

    /**
     * Map neo4j nodeId to inner nodeId
     * TODO rename?
     */
    long toHugeMappedNodeId(long nodeId);

    /**
     * Map inner nodeId back to original nodeId
     */
    long toOriginalNodeId(long nodeId);

    /**
     * Returns true iff the nodeId is mapped, otherwise false
     */
    boolean contains(long nodeId);

    /**
     * count of nodes
     */
    long hugeNodeCount();

}
