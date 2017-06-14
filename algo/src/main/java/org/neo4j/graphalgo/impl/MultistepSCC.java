package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class MultistepSCC {

    private final Graph graph;
    private final int cutOff;
    private IntHashSet nodeSet;
    private final Traverse traverse;
    private final int[] inDegree;
    private final int[] outDegree;
    private final MultiStepColoring coloring;
    private final SCCTarjan tarjan;

    public MultistepSCC(Graph graph, ExecutorService executorService, int concurrency, int cutOff) {
        this.graph = graph;
        this.cutOff = cutOff;
        this.inDegree = new int[graph.nodeCount()];
        this.outDegree = new int[graph.nodeCount()];
        traverse = new Traverse(graph);
        coloring = new MultiStepColoring(graph, executorService, concurrency) {
            @Override
            public void processSCC(int rootNodeId, IntSet scc) {
                MultistepSCC.this.processSCC(rootNodeId, scc);
            }
        };
        tarjan = new SCCTarjan(graph);

        init();
    }

    private void init() {
        nodeSet = new IntHashSet();

        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            nodeSet.add(i);
            inDegree[i] = graph.degree(i, Direction.INCOMING);
            outDegree[i] = graph.degree(i, Direction.OUTGOING);
        }
    }

    public void compute() {
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

        System.out.println("resulting nodeSet = " + nodeSet);

        while (!nodeSet.isEmpty()) {
            System.out.println("computing nodeSet = " + nodeSet);
            coloring.compute(nodeSet);
            System.out.println("resulting nodeSet = " + nodeSet);

        }


    }

    private void processSCC(int root, IntSet elements) {
        System.out.println(root + " -> " + elements);
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


}
