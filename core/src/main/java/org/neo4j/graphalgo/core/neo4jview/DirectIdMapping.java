package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.graphalgo.api.IdMapping;

/**
 * @author mh
 * @since 10.07.17
 */
public class DirectIdMapping implements IdMapping {
    private final long nodeCount;

    public DirectIdMapping(long nodeCount) {
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
    public long nodeCount() {
        return nodeCount;
    }
}
