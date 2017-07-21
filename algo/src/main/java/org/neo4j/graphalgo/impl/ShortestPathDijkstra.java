package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntMinPriorityQueue;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Dijkstra single source - single target shortest path algorithm
 *
 * The algorithm computes a (there might be more then one) shortest path
 * between a given start and target-NodeId. It returns result tuples of
 * [nodeId, distance] of each node in the path.
 *
 */
public class ShortestPathDijkstra extends Algorithm<ShortestPathDijkstra> {

    private final Graph graph;

    // node to cost map
    private final IntDoubleMap costs;
    // next node priority queue
    private final IntPriorityQueue queue;
    // auxiliary path map
    private final IntIntMap path;
    // path map (stores the resulting shortest path)
    private final IntArrayDeque finalPath;
    // visited set
    private final SimpleBitSet visited;
    private final int nodeCount;
    // overall cost of the path
    private double totalCost;
    // target node id
    private int goal;
    private ProgressLogger progressLogger;

    public ShortestPathDijkstra(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        costs = new IntDoubleScatterMap(nodeCount);
        queue = new SharedIntMinPriorityQueue(
                nodeCount,
                costs,
                Double.MAX_VALUE);
        path = new IntIntScatterMap(nodeCount);
        visited = new SimpleBitSet(nodeCount);
        finalPath = new IntArrayDeque();
        progressLogger = getProgressLogger();
    }

    /**
     * compute shortest path between startNode and goalNode
     * @return itself
     */
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
        totalCost = 0.0;
        while (last != -1) {
            finalPath.addFirst(last);
            last = path.getOrDefault(last, -1);
            totalCost += costs.getOrDefault(last, 0.0);
        }
        return this;
    }

    /**
     * return the result stream
     * @return stream of result DTOs
     */
    public Stream<Result> resultStream() {
        return StreamSupport.stream(finalPath.spliterator(), false)
                .map(cursor -> new Result(graph.toOriginalNodeId(cursor.value), costs.get(cursor.value)));
    }

    public IntArrayDeque getFinalPath() {
        return finalPath;
    }

    /**
     * get the distance sum of the path
     * @return sum of distances between start and goal
     */
    public double getTotalCost() {
        return totalCost;
    }

    /**
     * return the number of nodes the path consists of
     * @return number of nodes in the path
     */
    public int getPathLength() {
        return finalPath.size();
    }

    private void run(int goal) {
        while (!queue.isEmpty() && running()) {
            int node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.put(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    Direction.OUTGOING, (source, target, relId, weight) -> {
                        updateCosts(source, target, weight + costs);
                        if (!visited.contains(target)) {
                            queue.add(target, 0);
                        }
                        return true;
                    });
            progressLogger.logProgress((double) node / (nodeCount - 1));
        }
    }

    private void updateCosts(int source, int target, double newCosts) {
        double oldCosts = costs.getOrDefault(target, Double.MAX_VALUE);
        if (newCosts < oldCosts) {
            costs.put(target, newCosts);
            path.put(target, source);
        }
    }

    @Override
    public ShortestPathDijkstra me() {
        return this;
    }

    /**
     * Result DTO
     */
    public static class Result {

        /**
         * the neo4j node id
         */
        public final Long nodeId;
        /**
         * cost to reach the node from startNode
         */
        public final Double cost;

        public Result(Long nodeId, Double cost) {
            this.nodeId = nodeId;
            this.cost = cost;
        }
    }

    public static class SPExporter extends AbstractExporter<IntArrayDeque> {

        private final IdMapping idMapping;
        private final int propertyId;

        public SPExporter(IdMapping idMapping, GraphDatabaseAPI api, String propertyName) {
            super(api);
            this.idMapping = idMapping;
            propertyId = getOrCreatePropertyId(propertyName);
        }

        @Override
        public void write(IntArrayDeque data) {
            writeInTransaction(writeOp -> {
                int distance = 0;
                while (!data.isEmpty()) {
                    final int node = data.removeFirst();
                    try {
                        writeOp.nodeSetProperty(idMapping.toOriginalNodeId(node),
                                DefinedProperty.numberProperty(propertyId, distance++));
                    } catch (EntityNotFoundException | ConstraintValidationKernelException | InvalidTransactionTypeKernelException | AutoIndexingKernelException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

}
