package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class AllShortestPaths {

    private final Graph graph;
    private final int nodeCount;
    private final int concurrency;
    private final AtomicInteger queue;
    private final ExecutorService executorService;

    private double[][] paths;

    public AllShortestPaths(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >0");
        }
        this.concurrency = concurrency;
        this.queue = new AtomicInteger(nodeCount);
    }

    public AllShortestPaths compute() {
        paths = new double[nodeCount][];
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            runnables.add(new ShortestPathTask());
        }
        ParallelUtil.run(runnables, executorService);
        return this;
    }

    private class ShortestPathTask implements Runnable {

        @Override
        public void run() {
            int startNode;
            while ((startNode = queue.decrementAndGet()) >= 0) {
                paths[startNode] = new double[nodeCount];
                Arrays.fill(paths[startNode], Double.POSITIVE_INFINITY);
                paths[startNode][startNode] = 0d;
                graph.forEachNode(node -> {
                    final double sourceDistance = paths[startNode][node];
                    // scan relationships
                    graph.forEachRelationship(
                            node,
                            Direction.OUTGOING,
                            (source, target, relId, weight) -> {
                                // relax
                                final double targetDistance = weight + sourceDistance;
                                if (targetDistance < paths[startNode][target]) {
                                    paths[startNode][target] = targetDistance;
                                }
                                return true;
                            });
                    return true;
                });
            }
        }
    }

}
