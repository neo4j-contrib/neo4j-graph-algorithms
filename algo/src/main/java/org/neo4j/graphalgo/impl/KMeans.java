package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.MinimumSpanningTree;

/**
 * @author mknblch
 */
public class KMeans {

    private final MinimumSpanningTree tree;

    public KMeans(MinimumSpanningTree tree) {
        this.tree = tree;
    }

    /**
     * split the MST into k clusters
     * @param k
     * @return
     */
    public MinimumSpanningTree[] compute(int k) {
        return tree.split(k);
    }
}
