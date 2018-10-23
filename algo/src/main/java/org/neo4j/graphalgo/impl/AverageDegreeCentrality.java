package org.neo4j.graphalgo.impl;

import org.HdrHistogram.DoubleRecorder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class AverageDegreeCentrality extends Algorithm<AverageDegreeCentrality> {
    private final int nodeCount;
    private Direction direction;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();
    private DoubleRecorder doubleRecorder;

    public AverageDegreeCentrality(
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
        this.doubleRecorder = new DoubleRecorder(5);
    }

    public AverageDegreeCentrality compute() {
        nodeQueue.set(0);

        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
                tasks.add(new DegreeTask());
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        return this;
    }

    @Override
    public AverageDegreeCentrality me() {
        return this;
    }

    @Override
    public AverageDegreeCentrality release() {
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

                int degree = graph.degree(nodeId, direction);
                doubleRecorder.recordValue(degree);
            }
        }
    }

    public double average() {
        return doubleRecorder.getIntervalHistogram().getMean();
    }
}
