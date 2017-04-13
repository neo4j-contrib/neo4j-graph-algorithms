package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntMinPriorityQueue;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

public class ShortestPathDijkstra {

    private final IdMapping idMapping;
    private final WeightedOutgoingRelationshipIterator relationshipIterator;

    private final IntDoubleMap costs;
    private final IntPriorityQueue queue;
    private final IntIntMap path;
    private final IntArrayDeque finalPath;
    private final SimpleBitSet visited;

    public ShortestPathDijkstra(IdMapping idMapping, WeightedOutgoingRelationshipIterator relationshipIterator) {
        this.idMapping = idMapping;
        this.relationshipIterator = relationshipIterator;
        int nodeCount = idMapping.nodeCount();
        costs = new IntDoubleScatterMap(nodeCount);
        queue = new SharedIntMinPriorityQueue(
                nodeCount,
                costs,
                Double.MAX_VALUE);
        path = new IntIntScatterMap(nodeCount);
        finalPath = new IntArrayDeque();
        visited = new SimpleBitSet(nodeCount);
    }

    public int[] compute(long startNode, long goalNode) {
        visited.clear();
        queue.clear();

        int node = idMapping.toMappedNodeId(startNode);
        int goal = idMapping.toMappedNodeId(goalNode);
        costs.put(node, 0);
        queue.add(node, 0);
        run(goal);

        finalPath.clear();
        int last = goal;
        while (last != -1) {
            finalPath.addFirst(last);
            last = path.getOrDefault(last, -1);
        }

        return finalPath.toArray();
    }

    private void run(int goal) {
        while (!queue.isEmpty()) {
            int node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.put(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            relationshipIterator.forEachOutgoing(
                    node,
                    (source, target, relId, weight) -> {
                        updateCosts(source, target, weight + costs);
                        if (!visited.contains(target)) {
                            queue.add(target, 0);
                        }
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

}
