package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphdb.Path;

import java.util.ArrayList;
import java.util.List;

public class WalkResult {
    public Long startNodeId;
    public List<Long> nodeIds;
    public Path path;

    public WalkResult(long[] nodes, Path path) {
        this.startNodeId = nodes.length > 0 ? nodes[0] : null;
        this.nodeIds = new ArrayList<>(nodes.length);
        for (long node : nodes) this.nodeIds.add(node);
        this.path = path;
    }
}
