package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.graphalgo.api.IdMapping;

/**
 * @author mh
 * @since 10.07.17
 */
public class DirectIdMapping implements IdMapping {
    private final int nodeCount;

    public DirectIdMapping(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return Math.toIntExact(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(final long nodeId) {
        return true;
    }

    @Override
    public int nodeCount() {
        return nodeCount;
    }
}
