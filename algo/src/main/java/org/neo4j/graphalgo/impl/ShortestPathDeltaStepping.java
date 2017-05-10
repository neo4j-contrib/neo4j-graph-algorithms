package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Pair;

import java.util.Stack;
import java.util.function.Consumer;

public class ShortestPathDeltaStepping {

    private static final double MAX_VALUE = 9999d;

    private final IntDoubleMap tent;
    private final Buckets buckets;

    private final double delta;
    private final Graph graph;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.delta = delta;
        tent = new IntDoubleScatterMap();
        buckets = new Buckets();
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        for (int i = 0; i < graph.nodeCount(); i++) {
            tent.put(i, MAX_VALUE);
        }

        relax(graph.toMappedNodeId(startNode), 0d);

        final IntArrayDeque S = new IntArrayDeque();

        int i = 0;
        while (!buckets.isEmpty()) {
            S.clear();
            IntArrayDeque bi = buckets.get(i);

            System.out.println("===== phase " + i + " =====");
            while (!bi.isEmpty()) {

                int nodeId = bi.removeLast();

                System.out.println("nodeId = " + nodeId);

                final int b = i;
                S.addLast(nodeId);
                graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) {
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : light");
                        relax(targetNodeId, cost + tent.get(sourceNodeId));
                    }
                    return true;
                });
            }

            System.out.println("===== end phase " + i + " =====");

            S.forEach((IntProcedure) node -> {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost > delta) {
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : heavy");
                        relax(targetNodeId, cost + tent.get(sourceNodeId));
                    }
                    return true;
                });
            });


            i = buckets.nextNonEmptyBucket();
        }


        return this;
    }

    public LongDoubleMap getShortestPaths() {

        LongDoubleHashMap map = new LongDoubleHashMap();
        for (IntDoubleCursor cursor : tent) {
            map.put(graph.toOriginalNodeId(cursor.key), cursor.value);
        }

        return map;
    }

    private void relax(int nodeId, double cost) {
        if (cost >= tent.get(nodeId)) {
            return;
        }
        int cd = (int) ((double)tent.get(nodeId) / delta);
        System.out.print("removing " + nodeId + " from bucket " + cd + " : ");
        if (buckets.remove(cd, nodeId)) {
            System.out.println("successful");
        } else {
            System.out.println("failed");
        }
        int xd = (int) (cost / delta);
        System.out.println("pushing " + nodeId + " into bucket " + xd);
        buckets.set(xd, nodeId);
        System.out.println("set cost for " + nodeId + " to " + cost);
        tent.put(nodeId, cost);
    }
}
