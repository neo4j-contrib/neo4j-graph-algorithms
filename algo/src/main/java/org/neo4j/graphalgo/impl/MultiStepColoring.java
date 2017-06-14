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
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public abstract class MultiStepColoring {

    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final AtomicBitSet visited;
    private final List<Future<IntContainer>> futures = new ArrayList<>();
    private final Traverse traverse;

    private final int concurrency;

    private final int nodeCount;

    public MultiStepColoring(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.concurrency = concurrency;
        colors = new AtomicIntegerArray(nodeCount);
        visited = new AtomicBitSet(nodeCount);
        traverse = new Traverse(graph);
    }

    public abstract void processSCC(int rootNodeId, IntSet scc);

    public void compute(IntSet nodeSet) {

        resetColor();

//        final int size = nodeSet.size();
//        final int batchSize = Math.floorDiv(size, concurrency);
//        futures.clear();
//        futures.add(executorService.submit(() -> color(nodeSet)));
//        if (batchSize == 0) {
//        } else {
//            final Iterator<IntCursor> it = nodeSet.iterator();
//            for (int i = 0; i < size; i += batchSize) {
//                final IntScatterSet partition = partition(it, batchSize);
//                futures.add(executorService.submit(() -> color(partition)));
//            }
//        }
//        nodeSet.clear();
        // barrier sync (union all future results)
//        union(nodeSet, futures);

        color(nodeSet);

        forEachColor(color -> {
            // SCC(cv) <- PRED(G(cv), c)
            final IntSet scc = pred(nodeSet, color);
            processSCC(color, scc);
            // G <- G \ SCCc
            nodeSet.retainAll((IntLookupContainer) scc);
        });
    }

    /**
     * traverse backwards and collect all connected nodes with the same color
     * @param nodes
     * @param cv denotes the startNodeId and the color
     * @return set with all nodes reachable backwards with the same color as the startNode (is an SCC)
     */
    private IntSet pred(final IntSet nodes, final int cv) {
        final IntScatterSet set = new IntScatterSet();
        traverse.bfs(cv, Direction.INCOMING, nodes::contains, node -> {
            if (colors.get(node) == cv) {
                set.add(node);
            }
        });
        return set;
    }

    /**
     * for each distinct color
     *
     * @param consumer color consumer
     */
    private void forEachColor(IntConsumer consumer) {
        final IntScatterSet tColor = new IntScatterSet();
        for (int i = 0; i < nodeCount; i++) {
            tColor.add(colors.get(i));
        }
        tColor.forEach((IntProcedure) consumer::accept);
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
        while (!nodes.isEmpty()) {

            nodes.forEach((IntProcedure) node -> {

                System.out.println("node = " + node);
                final int nodeColor = colors.get(node);
                changed[0] = false;
                // for each <v, u> in E(V) (direction not specified)
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    // a higher color has been set by another thread, continue with next vertex
                    if (colors.get(targetNodeId) >= nodeColor) {
                        return true;
                    }
                    colors.set(targetNodeId, nodeColor);
                    // color has been assigned
                    changed[0] |= true;
                    // visit & add to level queue
                    if (!visited.get(targetNodeId)) {
                        visited.set(targetNodeId);
                        levelQueue.add(targetNodeId);
                    }
                    return true;
                });
                // if any u changed color process again
                if (changed[0] && !visited.get(node)) {
                    levelQueue.add(node);
                    visited.set(node);
                }
            });

            levelQueue.forEach((IntProcedure) visited::unset);
            nodes.clear();
            levelQueue.forEach((IntProcedure) nodes::add);
        }


        System.out.println("levelQueue = " + levelQueue);
        printColor();

        System.out.println("nodes = " + nodes);
        return levelQueue;
    }

    private void color2(IntSet nodes) {

        final IntScatterSet levelQueue = new IntScatterSet();
        final boolean[] changed = {false};
        do {
            changed[0] = false;
            nodes.forEach((IntProcedure) node -> {
                final int nodeColor = colors.get(node);
                // for each <v, u> in E(V) (direction not specified)
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    if (cas(targetNodeId, nodeColor)) {
                        changed[0] = true;
                    }
                    // visit & add to level queue
                    return true;
                });
            });

            System.out.println("changed[0] = " + changed[0]);
        } while (changed[0]);


        printColor();
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

    private IntSet union(IntSet ret, List<Future<IntContainer>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<IntContainer> future : futures) {
                try {
                    future.get().forEach((IntProcedure) ret::add);

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
        return ret;
    }

    private void printColor() {
        System.out.println("colors:");
        for (int i = 0; i < colors.length(); i++) {
            System.out.printf("%d -> %d%n", i, colors.get(i));
        }
    }
    private static void print(Object txt) {
        System.out.printf("[%s] %s%n", Thread.currentThread().getName(), txt);
    }
}
