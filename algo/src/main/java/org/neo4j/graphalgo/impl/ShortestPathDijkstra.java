package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntMinPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Dijkstra single source - single target shortest path algorithm
 * <p>
 * The algorithm computes a (there might be more then one) shortest path
 * between a given start and target-NodeId. It returns result tuples of
 * [nodeId, distance] of each node in the path.
 */
public class ShortestPathDijkstra extends Algorithm<ShortestPathDijkstra> {

    private static final int PATH_END = -1;
    public static final double NO_PATH_FOUND = -1.0;

    private Graph graph;

    // node to cost map
    private IntDoubleMap costs;
    // next node priority queue
    private IntPriorityQueue queue;
    // auxiliary path map
    private IntIntMap path;
    // path map (stores the resulting shortest path)
    private IntArrayDeque finalPath;
    // visited set
    private SimpleBitSet visited;
    private final int nodeCount;
    // overall cost of the path
    private double totalCost;
    // target node id
    private int goal;
    private ProgressLogger progressLogger;

    public ShortestPathDijkstra(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
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
     *
     * @return itself
     */
    public ShortestPathDijkstra compute(long startNode, long goalNode) {
        return compute(startNode, goalNode, Direction.BOTH);
    }

    public ShortestPathDijkstra compute(long startNode, long goalNode, Direction direction) {
        visited.clear();
        queue.clear();
        int node = graph.toMappedNodeId(startNode);
        goal = graph.toMappedNodeId(goalNode);
        costs.put(node, 0.0);
        queue.add(node, 0.0);
        run(goal, direction);
        finalPath.clear();
        totalCost = NO_PATH_FOUND;
        if (!path.containsKey(goal)) {
            return this;
        }
        int last = goal;
        totalCost = 0.0;
        while (last != PATH_END) {
            finalPath.addFirst(last);
            last = path.getOrDefault(last, PATH_END);
            totalCost += costs.get(last);
        }
        return this;
    }

    /**
     * return the result stream
     *
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
     *
     * @return sum of distances between start and goal
     */
    public double getTotalCost() {
        return totalCost;
    }

    /**
     * return the number of nodes the path consists of
     *
     * @return number of nodes in the path
     */
    public int getPathLength() {
        return finalPath.size();
    }

    private void run(int goal, Direction direction) {
        while (!queue.isEmpty() && running()) {
            int node = queue.pop();
            if (node == goal) {
                return;
            }

            visited.put(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    direction, (source, target, relId, weight) -> {
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

    @Override
    public ShortestPathDijkstra release() {
        graph = null;
        costs = null;
        queue = null;
        path = null;
        finalPath = null;
        visited = null;
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
                    } catch (KernelException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

}
