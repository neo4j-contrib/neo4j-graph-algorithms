package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class LouvainClustering extends Algorithm<LouvainClustering> {

    private final Graph graph;
    private final ExecutorService executorService;
    private final int concurrency;

    public LouvainClustering(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
    }

    public LouvainClustering compute() {

        return this;
    }

    @Override
    public LouvainClustering me() {
        return this;
    }

    @Override
    public LouvainClustering release() {
        return this;
    }

}
