package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.IntMinPriorityQueue;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ShortestPaths {

    private final Graph graph;
    private final IntDoubleMap costs;
    private final IntMinPriorityQueue queue;

    public ShortestPaths(Graph graph) {
        this.graph = graph;
        int nodeCount = graph.nodeCount();
        costs = new IntDoubleScatterMap(nodeCount);
        queue = new IntMinPriorityQueue();
    }

    public ShortestPaths compute(long startNode) {
        graph.forEachNode(node -> {
            costs.put(node, Double.POSITIVE_INFINITY);
            return true;
        });
        final int nodeId = graph.toMappedNodeId(startNode);
        costs.put(nodeId, 0d);
        queue.add(nodeId, 0d);
        run();
        return this;
    }

    /**
     * scale down integer representation to double[]
     * @return mapped-id to costSum array
     */
    public IntDoubleMap getShortestPaths() {
        return costs;
    }

    public Stream<Result> resultStream() {
        return StreamSupport.stream(costs.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.key), cursor.value));
    }

    private void run() {
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            double sourceCosts = this.costs.getOrDefault(node, Double.POSITIVE_INFINITY);
            // scan ALL relationships
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (source, target, relId, weight) -> {
                        // relax
                        final double targetCosts = this.costs.getOrDefault(target, Double.POSITIVE_INFINITY);
                        if (weight + sourceCosts < targetCosts) {
                            costs.put(target, weight + sourceCosts);
                            queue.add(target, targetCosts);
                        }
                        return true;
                    });

        }
    }

    public static class Result {

        public final long nodeId;
        public final double distance;

        public Result(Long nodeId, Double distance) {
            this.nodeId = nodeId;
            this.distance = distance;
        }
    }
}
