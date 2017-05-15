package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Parallel Delta-Stepping Non-negative Single Source Shortest Path
 */
public class ShortestPathDeltaStepping {

    private final AtomicIntegerArray distance;
    private final Buckets buckets;

    private final double delta;
    private final Graph graph;

    private final Collection<Runnable> light, heavy;
    private final Collection<Future<?>> futures;

    private ExecutorService executorService;

    private final double multiplier = 100_000d;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.delta = delta;
        distance = new AtomicIntegerArray(graph.nodeCount());
        buckets = new Buckets(graph.nodeCount());
        heavy = new ArrayDeque<>(1024);
        light = new ArrayDeque<>(1024);
        futures = new ArrayDeque<>(128);
    }

    public ShortestPathDeltaStepping withExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        // reset
        for (int i = 0; i < graph.nodeCount(); i++) {
            distance.set(i, Integer.MAX_VALUE);
        }
        buckets.reset();

        // basically assign start node to bucket 0
        relax(graph.toMappedNodeId(startNode), 0d);

        // as long as the bucket contains any value
        while (!buckets.isEmpty()) {
            // reset temporary arrays
            light.clear();
            heavy.clear();

            // get next bucket index
            final int phase = buckets.nextNonEmptyBucket();

            // for each node in bucket
            buckets.forEachInBucket(phase, node -> {
                // relax each outgoing light edge
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) { // determine if light or heavy edge
                        light.add(() -> relax(targetNodeId, cost + get(sourceNodeId)));
                    } else {
                        heavy.add(() -> relax(targetNodeId, cost + get(sourceNodeId)));
                    }
                    return true;
                });
                return true;
            });
            futures.clear();
            ParallelUtil.run(light, executorService, futures);
            futures.clear();
            ParallelUtil.run(heavy, executorService, futures);
        }

        return this;
    }

    private double get(int nodeId) {
        return distance.get(nodeId) / multiplier;
    }

    /**
     * compare and set
     * @param nodeId
     * @param cost
     */
    private void cas(int nodeId, double cost) {
        boolean stored = false;
        int c = (int) (cost * multiplier);
        while (!stored) {
            int oldC = distance.get(nodeId);
            if (c < oldC) {
                stored = distance.compareAndSet(nodeId, oldC, c);
            } else {
                break;
            }
        }
    }

    /**
     * relax() sets the summed cost in {@link ShortestPathDeltaStepping#distance}
     * if they are smaller then the current cost - like dijkstra. If so it also
     * calculates the next bucket index for the node and assigns it.
     *
     * @param nodeId node id
     * @param cost the summed cost
     */
    private void relax(int nodeId, double cost) {
        if (cost >= get(nodeId)) {
            return;
        }
        int bucketIndex = (int) (cost / delta); // calculate bucket index
        buckets.set(nodeId, bucketIndex);
        cas(nodeId, cost);
    }

    public double[] getShortestPaths() {
        double[] d = new double[graph.nodeCount()];
        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            d[i] = get(i);
        }
        return d;
    }

    public Stream<DeltaSteppingResult> resultStream() {
        return IntStream.range(0, graph.nodeCount())
                .mapToObj(node ->
                        new DeltaSteppingResult(graph.toOriginalNodeId(node), get(node)));
    }

    public static class DeltaSteppingResult {

        public final Long nodeId;
        public final Double distance;

        public DeltaSteppingResult(Long nodeId, Double distance) {
            this.nodeId = nodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "DeltaSteppingResult{" +
                    "nodeId=" + nodeId +
                    ", distance=" + distance +
                    '}';
        }
    }
}
