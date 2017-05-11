package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

public class ShortestPathDeltaStepping {

    private final double[] distance;
    private final Buckets buckets;

    private final double delta;
    private final Graph graph;
    private final IntSet heavySet;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.delta = delta;
        distance = new double[graph.nodeCount()];
        buckets = new Buckets(graph.nodeCount());
        heavySet = new IntScatterSet();
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        // reset
        Arrays.fill(distance, Double.MAX_VALUE);
        buckets.reset();

        // basically assign start node to bucket 0
        relax(graph.toMappedNodeId(startNode), 0d);

        // as long as the bucket contains any value
        while (!buckets.isEmpty()) {
            heavySet.clear();

            // get next bucket index
            final int phase = buckets.nextNonEmptyBucket();

            /*
             * evaluate for each node in bucket[phase]
             * (also clears the bucket BEFORE execution)
             */
            buckets.forEachInBucket(phase, node -> {
                // buffer the node id for heavy phase
                heavySet.add(node);
                // relax each outgoing light edge
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) { // determine if light edge
                        relax(targetNodeId, cost + distance[sourceNodeId]);
                    }
                    return true;
                });
                return true;
            });

            // for each buffered node relax their heavy edges
            heavySet.forEach((IntProcedure) node -> {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost > delta) { // eval only heavy edges since light ones have been relaxed in prev. step
                        relax(targetNodeId, cost + distance[sourceNodeId]);
                    }
                    return true;
                });
            });
        }

        return this;
    }

    public double[] getShortestPaths() {
        return distance;
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
}
