package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.ArrayBuckets;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphalgo.core.utils.container.HPPCBuckets;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

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
        buckets = new ArrayBuckets(graph.nodeCount());
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        for (int i = 0; i < graph.nodeCount(); i++) {
            tent.put(i, MAX_VALUE);
        }

        relax(graph.toMappedNodeId(startNode), 0d);

        final IntArrayDeque S = new IntArrayDeque();

        while (!buckets.isEmpty()) {

            S.clear();

            int phase = buckets.nextNonEmptyBucket();

            if (phase > 10000) {
                return this;
            }

            System.out.println("===== phase " + phase + " =====");
            buckets.forEachInBucket(phase, node -> {
                System.out.println("node = " + node);
                S.addLast(node);
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) {
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : light");
                        relax(targetNodeId, cost + tent.get(sourceNodeId));
                    }
                    return true;
                });
                return true;
            });
            System.out.println("===== end phase " + phase + " =====");

            S.forEach((IntProcedure) node -> {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost > delta) {
                        System.out.println(sourceNodeId + " -- " + cost + " -> " + targetNodeId + " : heavy");
                        relax(targetNodeId, cost + tent.get(sourceNodeId));
                    }
                    return true;
                });
            });


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

        int xd = (int) (cost / delta);
        System.out.println("setting " + nodeId + " into bucket " + xd);
        buckets.add(nodeId, xd);
        System.out.println("add cost for " + nodeId + " to " + cost);
        tent.put(nodeId, cost);
    }
}
