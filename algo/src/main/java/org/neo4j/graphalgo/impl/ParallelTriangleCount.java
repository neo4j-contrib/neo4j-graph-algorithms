package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.queue.IntMaxPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author mknblch
 */
public class ParallelTriangleCount extends Algorithm<ParallelTriangleCount> {

    // gapi
    private final Graph graph;
    // executor
    private final ExecutorService executorService;
    // number of threads to spawn
    private final int concurrency;
    private final int nodeCount;
    // node queue
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    private final IntObjectMap<BitSet> neighbors;
    private final int[] triangles;

    public ParallelTriangleCount(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        triangles = new int[nodeCount];
    }

    public int getTriangleCount() {
        return StreamSupport.stream(Arrays.spliterator(triangles), true)
                .mapToInt(k -> k).sum();
    }

    public int[] getTriangles() {
        return triangles;
    }

    @Override
    public ParallelTriangleCount me() {
        return this;
    }

    @Override
    public ParallelTriangleCount release() {
        return null;
    }

    public ParallelTriangleCount compute() {

        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                if (graph.degree(sourceNodeId, Direction.OUTGOING) < graph.degree(targetNodeId, Direction.OUTGOING)) {
                    addNeighbor(sourceNodeId, targetNodeId);
                } else {
                    addNeighbor(targetNodeId, sourceNodeId);
                }
                return true;
            });
            queue.add(i, graph.degree(i, Direction.OUTGOING));
        });

        final ArrayList<Task> tasks = new ArrayList<>();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, 1);
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int end = Math.min(i + batchSize, nodeCount);
            tasks.add(new Task(i, end));
        }
        ParallelUtil.run(tasks, executorService);
        return this;
    }

    private void addNeighbor(int source, int target) {
        if (!neighbors.containsKey(source)) {
            neighbors.put(source, new BitSet());
        }
        neighbors.get(source).set(target);
    }


    private class Task implements Runnable {

        private final int startIndex;
        private final int endIndex;
        private final IntMaxPriorityQueue queue;

        private int tCount = 0;

        private Task(int startIndex, int endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            neighbors = new IntObjectScatterMap<>(endIndex - startIndex);
            queue = new IntMaxPriorityQueue(endIndex - startIndex);
            initialize();
        }


        private void initialize() {
            for (int i = startIndex; i < endIndex && running(); i++) {
                graph.forEachRelationship(i, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (graph.degree(sourceNodeId, Direction.OUTGOING) < graph.degree(targetNodeId, Direction.OUTGOING)) {
                        addNeighbor(sourceNodeId, targetNodeId);
                    } else {
                        addNeighbor(targetNodeId, sourceNodeId);
                    }
                    return true;
                });
                queue.add(i, graph.degree(i, Direction.OUTGOING));
            }
        }

        @Override
        public void run() {
            queue.forEach(node -> {
                graph.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                    if (graph.degree(sourceNodeId, Direction.OUTGOING) < graph.degree(targetNodeId, Direction.OUTGOING)) {
                        final BitSet clone = (BitSet) neighbors.get(sourceNodeId).clone();
                        clone.intersect(neighbors.get(targetNodeId));
                        final BitSetIterator iterator = clone.iterator();
                        for (;;) {
                            final int v = iterator.nextSetBit();
                            triangles[node]++;
                        }

                    }
                    return true;
                });
                return true;
            });

        }
    }
}
