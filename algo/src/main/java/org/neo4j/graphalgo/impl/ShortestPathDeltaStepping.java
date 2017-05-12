package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Parallel Delta-Stepping Non-negative Single Source Shortest Path
 */
public class ShortestPathDeltaStepping {

    private final double[] distance;
    private final Buckets buckets;

    private final double delta;
    private final Graph graph;

    private final Collection<Runnable> light, heavy;

    private ExecutorService executorService;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.delta = delta;
        distance = new double[graph.nodeCount()];
        buckets = new Buckets(graph.nodeCount());
        heavy = new Stack<>();
        light = new Stack<>();
    }

    public ShortestPathDeltaStepping withExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        // reset
        Arrays.fill(distance, Double.MAX_VALUE);
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
                        light.add(() -> relax(targetNodeId, cost + distance[sourceNodeId]));
                    } else {
                        heavy.add(() -> relax(targetNodeId, cost + distance[sourceNodeId]));
                    }
                    return true;
                });
                return true;
            });
            ParallelUtil.run(light, executorService);
            ParallelUtil.run(heavy, executorService);
        }

        return this;
    }

    public double[] getShortestPaths() {
        return distance;
    }

    public Stream<DeltaSteppingResult> resultStream() {
        return IntStream.range(0, distance.length)
                .mapToObj(node ->
                        new DeltaSteppingResult(graph.toOriginalNodeId(node), distance[node]));
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
        if (cost >= distance[nodeId]) {
            return;
        }
        int bucketIndex = (int) (cost / delta); // calculate bucket index
        buckets.set(nodeId, bucketIndex);
        distance[nodeId] = cost;
    }

    public static class DeltaSteppingResult {

        public final Long nodeId;
        public final Double distance;

        public DeltaSteppingResult(Long nodeId, Double distance) {
            this.nodeId = nodeId;
            this.distance = distance;
        }
    }
}
