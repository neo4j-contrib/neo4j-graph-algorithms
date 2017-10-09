package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeIdMapping;

public final class HugeDirectIdMapping implements HugeIdMapping {
    private final long nodeCount;

    public HugeDirectIdMapping(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public long toHugeMappedNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeId < nodeCount;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }
}
