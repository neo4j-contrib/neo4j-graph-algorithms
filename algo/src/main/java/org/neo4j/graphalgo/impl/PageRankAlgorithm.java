package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.exporter.PageRankResult;

import java.util.concurrent.ExecutorService;

public interface PageRankAlgorithm {

    PageRankAlgorithm compute(int iterations);

    PageRankResult result();

    Algorithm<?> algorithm();

    static PageRankAlgorithm of(
            Graph graph,
            double dampingFactor) {
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(huge, huge, huge, huge, dampingFactor);
        }
        return new PageRank(graph, graph, graph, graph, dampingFactor);
    }

    static PageRankAlgorithm of(
            Graph graph,
            double dampingFactor,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        if (graph instanceof HugeGraph) {
            HugeGraph huge = (HugeGraph) graph;
            return new HugePageRank(
                    pool,
                    concurrency,
                    batchSize,
                    huge,
                    huge,
                    huge,
                    huge,
                    dampingFactor);
        }
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                graph,
                graph,
                graph,
                graph,
                dampingFactor);
    }
}
