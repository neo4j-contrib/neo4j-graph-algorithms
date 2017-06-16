package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class MultistepSCC {


    private final Graph graph;
    private final ExecutorService executorService;
    private final Traverse traverse;
    private final MultiStepColoring coloring;
    private final int concurrency;
    private final int cutOff;
    private final int nodeCount;
    private final int[] inDegree;
    private final int[] outDegree;
    private final MultiStepTrim trimming;

    public MultistepSCC(Graph graph, ExecutorService executorService, int concurrency, int cutOff) {

        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.cutOff = cutOff;
        this.nodeCount = graph.nodeCount();

        trimming = new MultiStepTrim(graph);
        coloring = new MultiStepColoring(graph, executorService, concurrency);
        traverse = new Traverse(graph);

        this.inDegree = new int[nodeCount];
        this.outDegree = new int[nodeCount];

    }

    public void compute() {
        // V <- simpleTrim (V)
        final IntSet nodeSet = trimming.compute(false);
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

        System.out.println("pre coloring nodeset = " + nodeSet);
        coloring.compute(nodeSet);
        final AtomicIntegerArray colors = coloring.getColors();

        System.out.println("post coloring nodeset = " + nodeSet);

        coloring.forEachColor(color -> {
            System.out.println("color = " + color);
            // SCC(cv) <- PRED(G(cv), c)
            final IntSet scc = pred(nodeSet, colors, color);
            processSCC(color, scc);
            // G <- G \ SCCc
            nodeSet.removeAll((IntLookupContainer) scc);
            return !nodeSet.isEmpty();
        });

        // TODO cutoff & tarjan
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
    private IntSet pred(final IntSet nodes, AtomicIntegerArray colors, final int cv) {
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

    public IntScatterSet partition(Iterator<IntCursor> it, int batchSize) {
        final IntScatterSet partition = new IntScatterSet(batchSize);
        for (int j = 0; j < batchSize && it.hasNext(); j++) {
            partition.add(it.next().value);
        }
        return partition;
    }

}
