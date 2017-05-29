package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ShortestPaths {

    private final Graph graph;
    private final IntDoubleMap costs;

    public ShortestPaths(Graph graph) {
        this.graph = graph;
        int nodeCount = graph.nodeCount();
        costs = new IntDoubleScatterMap(nodeCount);
    }

    public ShortestPaths compute(long startNode) {
        graph.forEachNode(node -> {
            costs.put(node, Double.POSITIVE_INFINITY);
            return true;
        });
        costs.put(graph.toMappedNodeId(startNode), 0d);
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
        graph.forEachNode(node -> {
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
                        }
                        return true;
                    });
            return true;
        });
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
