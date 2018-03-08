package org.neo4j.graphalgo.impl.yens;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphdb.Direction;

import java.util.Optional;

/**
 * @author mknblch
 */
public class Dijkstra {

    private static final int PATH_END = -1;

    private final HeavyGraph graph;
    private final int nodeCount;

    private final IntDoubleMap intermediateCosts;
    // node to cost map
    private final IntDoubleMap costs;
    // next node priority queue
    private final IntPriorityQueue queue;
    // auxiliary path map
    private final IntIntMap path;
    // visited set
    private final SimpleBitSet visited;

    private RelationshipConsumer filter = (sourceNodeId, targetNodeId, relationId) -> true;

    private Direction direction = Direction.BOTH;

    private ProgressLogger progressLogger = ProgressLogger.NULL_LOGGER;

    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public Dijkstra(HeavyGraph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        costs = new IntDoubleScatterMap(nodeCount);
        queue = SharedIntPriorityQueue.min(nodeCount,
                costs,
                Double.MAX_VALUE);
        path = new IntIntScatterMap(nodeCount);
        visited = new SimpleBitSet(nodeCount);
        intermediateCosts = new IntDoubleScatterMap();
    }

    public Dijkstra withoutFilter() {
        this.filter = (sourceNodeId, targetNodeId, relationId) -> true;
        return this;
    }

    public Dijkstra withFilter(RelationshipConsumer filter) {
        this.filter = filter;
        return this;
    }

    public Dijkstra withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    public Optional<WeightedPath> compute(int sourceNode, int targetNode) {
        if (!dijkstra(sourceNode, targetNode, direction)) {
            return Optional.empty();
        }
        int last = targetNode;
        final WeightedPath resultPath = new WeightedPath();
        while (last != PATH_END) {
            resultPath.append(last);
            last = path.getOrDefault(last, PATH_END);
        }
        resultPath.revert();
        return Optional.of(resultPath);
    }

    private boolean dijkstra(int source, int target, Direction direction) {
        costs.clear();
        queue.clear();
        path.clear();
        visited.clear();
        intermediateCosts.clear();
        costs.put(source, 0.0);
        queue.add(source, 0.0);
        while (!queue.isEmpty()) {
            int node = queue.pop();
            if (node == target) {
                return true;
            }
            visited.put(node);
            double costs = this.costs.getOrDefault(node, Double.MAX_VALUE);
            graph.forEachRelationship(
                    node,
                    direction, (s, t, relId) -> {
                        if (!filter.accept(s, t, relId)) {
                            return true;
                        }
                        final double w = graph.weightOf(s, t);
                        updateCosts(s, t, w,w + costs);
                        if (!visited.contains(t)) {
                            queue.add(t, 0);
                        }
                        return true;
                    });
        }
        return false;
    }

    private void updateCosts(int source, int target, double weight, double newCosts) {
        double oldCosts = costs.getOrDefault(target, Double.MAX_VALUE);
        if (newCosts < oldCosts) {
            intermediateCosts.put(target, weight);
            costs.put(target, newCosts);
            path.put(target, source);
        }
    }
}
