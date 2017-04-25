package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntMinPriorityQueue;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ShortestPathDijkstra {

    private final Graph graph;

    private final IntDoubleMap costs;
    private final IntPriorityQueue queue;
    private final IntIntMap path;
    private final IntArrayDeque finalPath;
    private final SimpleBitSet visited;

    private double totalCost = 0.0;

    private int goal;

    public ShortestPathDijkstra(Graph graph) {
        this.graph = graph;
        int nodeCount = graph.nodeCount();
        costs = new IntDoubleScatterMap(nodeCount);
        queue = new SharedIntMinPriorityQueue(
                nodeCount,
                costs,
                Double.MAX_VALUE);
        path = new IntIntScatterMap(nodeCount);
        visited = new SimpleBitSet(nodeCount);
        finalPath = new IntArrayDeque();
    }

    public ShortestPathDijkstra compute(long startNode, long goalNode) {
        visited.clear();
        queue.clear();
        int node = graph.toMappedNodeId(startNode);
        goal = graph.toMappedNodeId(goalNode);
        costs.put(node, 0);
        queue.add(node, 0);
        run(goal);
        int last = goal;
        finalPath.clear();
        while (last != -1) {
            finalPath.addFirst(last);
            last = path.getOrDefault(last, -1);
            totalCost += costs.getOrDefault(last, 0.0);
        }
        return this;
    }

    public Stream<Result> resultStream() {
        return StreamSupport.stream(finalPath.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.value), costs.get(cursor.value)));
    }

    public double getTotalCost() {
        return totalCost;
    }

    public int getPathLength() {
        return finalPath.size();
    }

    private void run(int goal) {
        while (!queue.isEmpty()) {
            int node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.put(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING,
                    (WeightedRelationshipConsumer)(source, target, relId, weight) -> {
                        updateCosts(source, target, weight + costs);
                        if (!visited.contains(target)) {
                            queue.add(target, 0);
                        }
                        return true;
                    });
        }
    }

    private void updateCosts(int source, int target, double newCosts) {
        double oldCosts = costs.getOrDefault(target, Double.MAX_VALUE);
        if (newCosts < oldCosts) {
            costs.put(target, newCosts);
            path.put(target, source);
        }
    }


    public static class Result {

        public final Long nodeId;
        public final Double cost;

        public Result(Long nodeId, Double cost) {
            this.nodeId = nodeId;
            this.cost = cost;
        }
    }
}
