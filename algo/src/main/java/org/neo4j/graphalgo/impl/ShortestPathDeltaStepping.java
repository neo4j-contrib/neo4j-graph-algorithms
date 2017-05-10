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

    private final int nodeCount;
    private final double delta;
    private final Graph graph;

    public ShortestPathDeltaStepping(Graph graph, double delta) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.delta = delta;
        tent = new IntDoubleScatterMap();
        buckets = new Buckets(nodeCount);
    }

    public ShortestPathDeltaStepping compute(long startNode) {

        for (int i = 0; i < graph.nodeCount(); i++) {
            tent.put(i, MAX_VALUE);
        }

        relax(graph.toMappedNodeId(startNode), 0d);

        final IntArrayDeque S = new IntArrayDeque();
        final Stack<Pair<Integer, Double>> req = new Stack<>();

        int i = 0;
        while (!buckets.isEmpty()) {
            System.out.println("i = " + i);
            S.clear();
            IntArrayDeque bi = buckets.get(i);

            while (!bi.isEmpty()) {

                int nodeId = bi.removeLast();
                graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost <= delta) {
                        req.push(Pair.of(targetNodeId, cost + tent.get(sourceNodeId)));
                    }
                    return true;
                });
                S.addLast(nodeId);
                req.forEach(pair -> {
                    relax(pair.first(), pair.other());
                });
                req.clear();
            }

            S.forEach((IntProcedure) node -> {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, cost) -> {
                    if (cost > delta) {
                        req.push(Pair.of(targetNodeId, cost + tent.get(sourceNodeId)));
                    }
                    return true;
                });
            });

            req.forEach(pair -> {
                relax(pair.first(), pair.other());
            });
            req.clear();

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
        if (cost >= tent.getOrDefault(nodeId, Double.MAX_VALUE)) {
            return;
        }
        int xd = (int) (cost / delta);
        buckets.set(nodeId, xd);
        tent.put(nodeId, cost);
    }

}
