package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.queue.IntMinPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;

public class ShortestPathDijkstraKernel {

    private final Graph graph;

    private final double[] costs;
    private final IntMinPriorityQueue queue;
    private final int[] path;
    private final IntArrayDeque finalPath;
    private final BitSet visited;

    public ShortestPathDijkstraKernel(Graph graph) {
        this.graph = graph;
        int nodeCount = graph.nodeCount();
        costs = new double[nodeCount];
        queue = new IntMinPriorityQueue();
        path = new int[nodeCount];
        finalPath = new IntArrayDeque();
        visited = new BitSet(nodeCount);
    }

    public int[] compute(long startNode, long goalNode) {
        Arrays.fill(costs, Double.MAX_VALUE);
        Arrays.fill(path, -1);
        visited.clear();
        queue.clear();

        int node = graph.toMappedNodeId(startNode);
        int goal = graph.toMappedNodeId(goalNode);
        costs[node] = 0;
        queue.add(node, 0);
        run(goal);

        finalPath.clear();
        int last = goal;
        while (last != -1) {
            finalPath.addFirst(last);
            last = path[last];
        }

        return finalPath.toArray();
    }

    private void run(int goal) {
        while (!queue.isEmpty()) {
            int node = queue.pop();
            if (node == goal) {
                path[goal] = node;
                return;
            }
            visited.set(node);
            double costs = this.costs[node];
            graph.forEachRelationship(
                node,
                Direction.OUTGOING,
                    (WeightedRelationshipConsumer)(source, target, weight, edge) -> {
                    final double targetCost = updateCosts(source, target, weight, costs);
                    if (!visited.get(target)) {
                        queue.add(target, targetCost);
                    }
                    return true;
                });
        }
    }

    private double updateCosts(int source, int target, double weight, double collectedCosts) {
        final double newCosts = weight + collectedCosts;
        double oldCosts = costs[target];
        if (newCosts < oldCosts) {
            this.costs[target] = newCosts;
            path[target] = source;
            return newCosts;
        }
        return oldCosts;
    }

}
