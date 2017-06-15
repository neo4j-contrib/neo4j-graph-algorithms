package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.container.AtomicBitSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public class MultistepSCC {


    private final Graph graph;
    private final ExecutorService executorService;
    private final AtomicIntegerArray colors;
    private final Traverse traverse;
    private final MultiStepColoring coloring;
    private final int concurrency;
    private final int cutOff;
    private final int nodeCount;
    private final int[] inDegree;
    private final int[] outDegree;
    private IntSet nodeSet;

    public MultistepSCC(Graph graph, ExecutorService executorService, int concurrency, int cutOff) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.cutOff = cutOff;
        this.nodeCount = graph.nodeCount();
        this.inDegree = new int[nodeCount];
        this.outDegree = new int[nodeCount];
        coloring = new MultiStepColoring(graph, executorService, concurrency);
        colors = new AtomicIntegerArray(nodeCount);
        traverse = new Traverse(graph);
        init();

    }

    private void init() {
        nodeSet = new IntHashSet();

        for (int i = nodeCount - 1; i >= 0; i--) {
            nodeSet.add(i);
            inDegree[i] = graph.degree(i, Direction.INCOMING);
            outDegree[i] = graph.degree(i, Direction.OUTGOING);
        }
    }

    public void compute() {
        resetColors();
        // V <- simpleTrim (V)
        trim(false);
        System.out.println("nodeSet = " + nodeSet);
        final int rootPivot = pivot(nodeSet);
        System.out.println("rootPivot = " + rootPivot);
        // D <- BFS( G(V,E(V)), v)
        final IntScatterSet descendant = new IntScatterSet();
        traverse.bfs(rootPivot, Direction.OUTGOING, nodeSet::contains, descendant::add);
        System.out.println("descendant = " + descendant);
        // ST <- BFS( G(V, E'(V)), v)
        final IntSet rootSCC = new IntScatterSet();
        traverse.bfs(rootPivot, Direction.INCOMING, descendant::contains, rootSCC::add);
        // SCC <- V & ST
        rootSCC.retainAll(descendant);
        System.out.println("rootSCC = " + rootSCC);
        processSCC(rootPivot, rootSCC);
        // V <- V \ SCC
        nodeSet.removeAll((IntLookupContainer) rootSCC);

        // coloring2(nodeSet); // TODO

    }


    public IntSet coloring(IntSet nodeSet) {


        color(nodeSet);

        printColor();

        forEachColor(color -> {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // SCC(cv) <- PRED(G(cv), c)
            System.out.println("nodeSet = " + nodeSet);
            final IntSet scc = pred(nodeSet, color);
            System.out.println("scc = " + scc);
            processSCC(color, scc);
            // G <- G \ SCCc
            nodeSet.removeAll((IntLookupContainer) scc);
        });

        return nodeSet;
    }

    public void color(IntSet nodes) {



        final IntSet levelQueue = new IntScatterSet();
        final IntSet nodeQueue = new IntScatterSet();

        nodes.forEach((IntProcedure) levelQueue::add);
        nodes.forEach((IntProcedure) nodeQueue::add);

        final boolean[] changed = {false};
        resetColors();

        final SimpleBitSet visited = new SimpleBitSet(nodes.size());

        while (!nodeQueue.isEmpty()) {

            levelQueue.clear();

            nodeQueue.forEach((IntProcedure) node -> {

                final int nodeColor = colors.get(node);
                changed[0] = false;

                // for each <v, u> in E(V) (direction not specified)
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    // maybe set color
                    if (!cas(targetNodeId, nodeColor)) {
                        // a higher color has been set by another thread, continue with next vertex
                        return true;
                    }
                    changed[0] = true;
                    // visit & add to level queue
                    if (!visited.contains(targetNodeId)) {
                        visited.put(targetNodeId);
                        levelQueue.add(targetNodeId);
                    }
                    return true;
                });

                // if any node changed color process again
                if (changed[0] && !visited.contains(node)) {
                    levelQueue.add(node);
                    visited.put(node);
                }
            });

            nodeQueue.clear();

            levelQueue.forEach((IntProcedure) visited::remove);
            levelQueue.forEach((IntProcedure) nodeQueue::add);
        }



    }


    public IntSet coloring2(IntSet nodeSet) {

        color2(nodeSet);

        forEachColor(color -> {
            // SCC(cv) <- PRED(G(cv), c)
            System.out.println("nodeSet = " + nodeSet);
            final IntSet scc = pred(nodeSet, color);
            System.out.println("scc = " + scc);
            processSCC(color, scc);
            // G <- G \ SCCc
            nodeSet.removeAll((IntLookupContainer) scc);
        });

        return nodeSet;
    }

    public void color2(IntSet nodes) {

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

    // TODO
    private void processSCC(int root, IntSet elements) {
        if (elements.isEmpty()) {
            return;
        }
        System.out.println(root + " -> " + elements);
    }

    /**
     * find node with highest product of in- and out-degree
     * v E V for which Din(V) * Dout(V) is max
     * @param set the nodeSet
     * @return the nodeId
     */
    private int pivot(IntSet set) {
        int product = 0;
        int pivot[] = {0};
        set.forEach((IntProcedure) node -> {
            final int p = graph.degree(node, Direction.OUTGOING) * graph.degree(node, Direction.INCOMING);
            if (p > product) {
                pivot[0] = node;
            }
        });
        return pivot[0];
    }

    /**
     * traverse backwards and collect all connected nodes with the same color
     * @param nodes
     * @param cv denotes the startNodeId and the color
     * @return set with all nodes reachable backwards with the same color as the startNode (is an SCC)
     */
    private IntSet pred(final IntSet nodes, final int cv) {
        System.out.println("predecessor of cv = " + cv);
        System.out.println("nodes = " + nodes);
        final IntScatterSet set = new IntScatterSet();
        traverse.bfs(cv, Direction.INCOMING, nodes::contains, node -> {
            System.out.println("pred " + node + " with color " + colors.get(node) + " must match " + cv);
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

    /**
     * trim the graph by removing nodes with IN- or OUT-Degree of 0
     * and all self-referencing nodes without relationships to others.
     *
     * @param complete prune the graph until no more changes can be made
     *                 does only one iteration otherwise
     */
    private void trim(boolean complete) {
        final IntScatterSet remove = new IntScatterSet();
        boolean changes; // tells whether the last iteration changed the graph
        final boolean[] filter = {false};
        do {
            changes = false;
            for(Iterator<IntCursor> it = nodeSet.iterator(); it.hasNext(); ) {
                final int node = it.next().value;
                filter[0] = false;
                // rm nodes without incoming arcs and update target degrees
                if (inDegree[node] == 0) {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        outDegree[targetNodeId]--;
                        return true;
                    });
                    filter[0] = true;
                }
                // rm nodes without outgoing arcs and update target degrees
                if (inDegree[node] == 0) {
                    graph.forEachRelationship(node, Direction.INCOMING, (sourceNodeId, targetNodeId, relationId) -> {
                        inDegree[targetNodeId]--;
                        return true;
                    });
                    filter[0] = true;
                }
                // rm self loops
                if (inDegree[node] == 1 && outDegree[node] == 1) {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        if (sourceNodeId == targetNodeId) {
                            filter[0] = true;
                            return false;
                        }
                        return false;
                    });
                }
                // remove
                if (filter[0]) {
                    remove.add(node);
                }
                changes |= filter[0];
            }
            nodeSet.removeAll(remove);

        } while (changes && complete);
    }

    private void resetColors() {
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

}
