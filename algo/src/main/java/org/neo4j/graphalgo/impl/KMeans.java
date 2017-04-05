package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.MinimumSpanningTree;

/**
 * @author mknblch
 */
public class KMeans {

    private final Graph graph;

    public KMeans(Graph graph) {
        this.graph = graph;
    }

    /**
     * split the MST from startNode into k clusters
     * @param k
     * @return
     */
    public MinimumSpanningTree[] compute(int startNode, int k) {
        return new MSTPrim(graph).compute(startNode).split(k);
    }
}
