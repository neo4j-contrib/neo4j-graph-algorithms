package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntMinPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * AllShortestPaths:
 * <p>
 * multi-source parallel dijkstra algorithm for computing the shortest path between
 * each pair of nodes.
 * <p>
 * Since all nodeId's have already been ordered by the idMapping we can use an integer
 * instead of a queue which just count's up for each startNodeId as long as it is
 * < nodeCount. Each thread tries to take one int from the counter at one time and
 * starts its computation on it.
 * <p>
 * The {@link AllShortestPaths#concurrency} value determines the count of workers
 * that should be spawned.
 * <p>
 * Due to the high memory footprint the result set would have we emit each result into
 * a blocking queue. The result stream takes elements from the queue while the workers
 * add elements to it. The result stream is limited by N^2. If the stream gets closed
 * prematurely the workers get closed too.
 */
public class AllShortestPaths extends Algorithm<AllShortestPaths> {

    private final Graph graph;
    private final int nodeCount;

    /**
     * maximum number of workers
     */
    private final int concurrency;
    /**
     * nodeId counter (init with nodeCount,
     * counts down for each node)
     */
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

    /**
     * the resultStream(..) method starts the computation and
     * returns a Stream of SP-Tuples (source, target, minDist)
     *
     * @return the result stream
     */
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

    @Override
    public AllShortestPaths me() {
        return this;
    }

    /**
     * Dijkstra Task. Takes one element of the counter at a time
     * and starts dijkstra on it. It starts emitting results to the
     * queue once all reachable nodes have been visited.
     */
    private class ShortestPathTask implements Runnable {

        private final IntMinPriorityQueue queue;
        private final double[] distance;

        private ShortestPathTask() {
            distance = new double[nodeCount];
            queue = new IntMinPriorityQueue();
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
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
                progressLogger.logProgress((double) startNode / (nodeCount - 1));
            }
        }

        public void compute(int startNode) {
            Arrays.fill(distance, Double.POSITIVE_INFINITY);
            distance[startNode] = 0d;
            queue.add(startNode, 0d);
            while (running && !queue.isEmpty()) {
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

    /**
     * Result DTO
     */
    public static class Result {

        /**
         * neo4j nodeId of the source node
         */
        public final long sourceNodeId;
        /**
         * neo4j nodeId of the target node
         */
        public final long targetNodeId;
        /**
         * minimum distance between source and target
         */
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
