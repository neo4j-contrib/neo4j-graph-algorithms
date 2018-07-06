package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphdb.Path;

import java.util.ArrayList;
import java.util.List;

public class WalkResult {
    public Path path;
    public List<Long> nodes;

    public WalkResult(long[] nodes, Path path) {
        this.nodes = new ArrayList<>(nodes.length);
        for (long node : nodes) this.nodes.add(node);
        this.path = path;
    }
}
