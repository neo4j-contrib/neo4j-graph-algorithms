package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.IntMinPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class AllShortestPaths {

    private final Graph graph;
    private final int nodeCount;
    private final int concurrency;
    private final AtomicInteger counter;
    private final ExecutorService executorService;
    private final BlockingQueue<Result> resultQueue;

    private volatile boolean running;

    public AllShortestPaths(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >0");
        }
        this.concurrency = concurrency;
        this.counter = new AtomicInteger();
        this.resultQueue = new LinkedBlockingQueue<>(); // TODO limit size?
    }


    public Stream<Result> resultStream() {

        counter.set(0);
        running = true;

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(new ShortestPathTask());
        }

        long end = (long) nodeCount * nodeCount;

        return LongStream.range(0, end)
                .onClose(() -> running = false)
                .mapToObj(i -> {
                    try {
                        return resultQueue.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private class ShortestPathTask implements Runnable {

        private final IntMinPriorityQueue queue;
        private final double[] distance;

        private ShortestPathTask() {
            distance = new double[nodeCount];
            queue = new IntMinPriorityQueue();
        }

        @Override
        public void run() {
            int startNode;
            while (running && (startNode = counter.getAndIncrement()) < nodeCount) {
                compute(startNode);
                for (int i = 0; i < nodeCount; i++) {
                    final Result result = new Result(
                            graph.toOriginalNodeId(startNode),
                            graph.toOriginalNodeId(i),
                            distance[i]);

                    try {
                        resultQueue.put(result);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        public void compute(int startNode) {
            Arrays.fill(distance, Double.POSITIVE_INFINITY);
            distance[startNode] = 0d;
            queue.add(startNode, 0d);

            while (!queue.isEmpty()) {
                final int node = queue.pop();
                final double sourceDistance = distance[node];
                // scan relationships
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (source, target, relId, weight) -> {
                            // relax
                            final double targetDistance = weight + sourceDistance;
                            if (targetDistance < distance[target]) {
                                distance[target] = targetDistance;
                                queue.add(target, targetDistance);
                            }
                            return true;
                        });
            }
        }
    }

    public static class Result {

        public final long sourceNodeId;

        public final long targetNodeId;

        public final double distance;

        public Result(long sourceNodeId, long targetNodeId, double distance) {
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "sourceNodeId=" + sourceNodeId +
                    ", targetNodeId=" + targetNodeId +
                    ", distance=" + distance +
                    '}';
        }
    }
}
