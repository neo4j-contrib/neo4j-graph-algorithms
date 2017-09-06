package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.api.Graph;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class Community {

    private final Graph graph;
    private final int[] cluster;

    public Community(Graph graph) {
        this.graph = graph;
        this.cluster = new int[graph.nodeCount()];
    }

    public void reset() {
        Arrays.parallelSetAll(cluster, i -> i);
    }

    public IntIntMap modularityVector() {
        final IntIntScatterMap modularity = new IntIntScatterMap(graph.nodeCount());

//        modularity

        return modularity;
    }
}
