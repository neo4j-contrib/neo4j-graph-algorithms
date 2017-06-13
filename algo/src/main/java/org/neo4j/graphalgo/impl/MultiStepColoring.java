package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class MultiStepColoring {

    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final AtomicBitSet visited;
    private final List<Future<IntContainer>> futures = new ArrayList<>();

    private final int concurrency = 3;

    private final int nodeCount;

    public MultiStepColoring(Graph graph, ExecutorService executorService) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        colors = new AtomicIntegerArray(nodeCount);
        visited = new AtomicBitSet(nodeCount);
        resetColor();
    }

    private IntSet allNodes() {
        final IntHashSet intCursors = new IntHashSet(nodeCount);
        for (int i = nodeCount - 1; i >= 0; i--) {
            intCursors.add(i);
        }
        return intCursors;
    }

    public AtomicIntegerArray compute() {
        return compute(allNodes());
    }

    public AtomicIntegerArray compute(IntSet nodeSet) {

        resetColor();

        while (!nodeSet.isEmpty()) {

            final int size = nodeSet.size();
            final int batchSize = Math.floorDiv(size, concurrency);


            print("size = " + size);
            print("batchSize = " + batchSize);
            print("tempSet = " + nodeSet);
            futures.clear();

            if (batchSize == 0) {
                futures.add(executorService.submit(() -> color(nodeSet)));
            } else {
                final Iterator<IntCursor> it = nodeSet.iterator();
                for (int i = 0; i < size; i += batchSize) {
                    final IntScatterSet partition = partition(it, batchSize);
                    print("partition = " + partition);
                    futures.add(executorService.submit(() -> color(partition)));
                }
            }
            // barrier sync
            nodeSet.clear();
            union(nodeSet, futures);
        }
        return colors;
    }

    private void resetColor() {
        for (int i = 0; i < nodeCount; i++) {
            colors.set(i, i);
        }
    }

    public IntScatterSet partition(Iterator<IntCursor> it, int batchSize) {
        final IntScatterSet partition = new IntScatterSet(batchSize);
        for (int j = 0; j < batchSize && it.hasNext(); j++) {
            partition.add(it.next().value);
        }
        return partition;
    }

    private IntSet color(IntSet nodes) {

        final IntSet levelQueue = new IntScatterSet();
        final boolean[] changed = {false};
        nodes.forEach((IntProcedure) node -> {
            final int nodeColor = colors.get(node);
            print("inspecting node = " + node + " with nodeColor = " + nodeColor);
            changed[0] = false;
            // for each <v, u> in E(V) (direction not specified)
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                if (!cas(targetNodeId, nodeColor)) {
                    return true;
                }
                changed[0] |= true;
                if (!visited.get(targetNodeId)) {
                    visited.set(targetNodeId);
                    levelQueue.add(targetNodeId);
                }
                return true;
            });
            // if any u changed color
            if (changed[0] && !visited.get(node)) {
                levelQueue.add(node);
            }
        });

        levelQueue.forEach((IntProcedure) visited::unset);

        return levelQueue;
    }

    private boolean cas(int nodeId, int color) {
        boolean stored = false;
        while (!stored) {
            int oldC = colors.get(nodeId);
            if (color > oldC) {
                stored = colors.compareAndSet(nodeId, oldC, color);
            } else {
                break;
            }
        }
        return stored;
    }

    private IntSet union(IntSet set, List<Future<IntContainer>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<IntContainer> future : futures) {
                try {
                    future.get().forEach((IntProcedure) set::add);

                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
        return set;
    }

    private static void print(Object txt) {
        System.out.printf("[%s] %s%n", Thread.currentThread().getName(), txt);
    }
}
