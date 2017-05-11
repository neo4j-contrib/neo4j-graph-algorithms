package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ShortestPathDeltaStepping {

    private final double[] distance;
    private final Buckets buckets;

    private final double delta;
    private final Graph graph;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.delta = delta;
        distance = new double[graph.nodeCount()];
        buckets = new Buckets(graph.nodeCount());
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        Arrays.fill(distance, -1d);

        relax(graph.toMappedNodeId(startNode), 0d);

        final IntSet S = new IntScatterSet();

        int i = 0;

        while (!buckets.isEmpty()) {

            System.out.println("iteration " + i++);
            int phase = buckets.nextNonEmptyBucket();

            S.clear();

            System.out.println("\n===== phase " + phase + " =====");

            System.out.println("buckets = " + buckets);
            buckets.forEachInBucket(phase, node -> {
                System.out.println("node = " + node);
                S.add(node);
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) {
                        relax(targetNodeId, cost + tent(sourceNodeId));
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : light");
                    }
                    return true;
                });
                return true;
            });
            System.out.println("===== end phase " + phase + " =====");
            printTent();
            System.out.println("S = " + S);
            S.forEach((IntProcedure) node -> {
                System.out.println("buckets = " + buckets);

                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost > delta) {
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : heavy");

                        relax(targetNodeId, cost + tent(sourceNodeId));
                    }
                    return true;
                });
            });


        }


        return this;
    }

    private double tent(int node) {
        return distance[node] < 0 ? Double.MAX_VALUE : distance[node];
    }

    private void printTent() {
        System.out.println("distance = " + Arrays.stream(distance).mapToObj(d -> String.format("[%2.0f]", d)).collect(Collectors.joining(", ")));
    }

    public double[] getShortestPaths() {

        LongDoubleHashMap map = new LongDoubleHashMap();

        for (int i = 0; i < distance.length; i++) {
//            map.put(graph.toOriginalNodeId(cursor.key), cursor.value);
            map.put(i, distance[i]);

        }

        return distance;
    }

    private void relax(int nodeId, double cost) {
        if (cost >= tent(nodeId)) {
            return;
        }

        int xd = (int) (cost / delta);
        System.out.println("set " + nodeId + " to bucket " + xd + " with cost " + cost);
        buckets.set(nodeId, xd);
        distance[nodeId] = cost;
        printTent();

    }
}
