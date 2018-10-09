package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WeightedDegreeCentrality extends Algorithm<WeightedDegreeCentrality> {
    private final int nodeCount;
    private Direction direction;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    private double[] degrees;
    private double[][] weights;

    public WeightedDegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency,
            Direction direction
    ) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_QUEUE_SIZE;
        }

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.direction = direction;
        degrees = new double[nodeCount];
        weights = new double[nodeCount][];
    }

    public WeightedDegreeCentrality compute(boolean cacheWeights) {
        nodeQueue.set(0);

        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            if(cacheWeights) {
                tasks.add(new DegreeAndWeightsTask());
            } else {
                tasks.add(new DegreeTask());
            }
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        return this;
    }

    @Override
    public WeightedDegreeCentrality me() {
        return this;
    }

    @Override
    public WeightedDegreeCentrality release() {
        graph = null;
        return null;
    }

    private class DegreeTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                double[] weightedDegree = new double[1];
                graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    return true;
                });

                degrees[nodeId] = weightedDegree[0];

            }
        }
    }

    private class DegreeAndWeightsTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                weights[nodeId] = new double[graph.degree(nodeId, direction)];

                int[] index = {0};
                double[] weightedDegree = new double[1];
                graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId, relationId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    weights[nodeId][index[0]] = weight;
                    index[0]++;
                    return true;
                });

                degrees[nodeId] = weightedDegree[0];

            }
        }
    }

    public double[] degrees() {
        return degrees;
    }
    public double[][] weights() {
        return weights;
    }

    public Stream<DegreeCentrality.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new DegreeCentrality.Result(graph.toOriginalNodeId(nodeId), degrees[nodeId]));
    }

}
