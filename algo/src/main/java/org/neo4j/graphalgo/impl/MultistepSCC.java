package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * @author mknblch
 */
public class MultistepSCC {

    private final Graph graph;
    private final int cutOff;
    private IntHashSet nodeSet;

    private final int[] inDegree;
    private final int[] outDegree;

    public MultistepSCC(Graph graph, int cutOff) {
        this.graph = graph;
        this.cutOff = cutOff;
        this.inDegree = new int[graph.nodeCount()];
        this.outDegree = new int[graph.nodeCount()];
    }

    private void init() {
        nodeSet = new IntHashSet();

        for (int i = graph.nodeCount() - 1; i >= 0; i--) {
            nodeSet.add(i);
            inDegree[i] = graph.degree(i, Direction.INCOMING);
            outDegree[i] = graph.degree(i, Direction.OUTGOING);
        }
    }

    /**
     * Prune the graph by removing nodes with IN- or OUT-Degree of 0
     * and all self-referencing nodes without relationships to others.
     *
     * @param complete prune the graph until no more changes can be made
     *                 does only one iteration otherwise
     */
    private void prune(boolean complete) {

        boolean changes; // tells whether the last iteration changed the graph
        final Flag filter = new Flag(); // tells whether the node should be filtered out
        do {
            changes = false;
            for(Iterator<IntCursor> it = nodeSet.iterator(); it.hasNext(); ) {
                final int node = it.next().value;
                filter.value = false;
                // rm nodes without incoming arcs and update target degrees
                if (inDegree[node] == 0) {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        outDegree[targetNodeId]--;
                        return true;
                    });
                    filter.value = true;
                }
                // rm nodes without outgoing arcs and update target degrees
                if (inDegree[node] == 0) {
                    graph.forEachRelationship(node, Direction.INCOMING, (sourceNodeId, targetNodeId, relationId) -> {
                        inDegree[targetNodeId]--;
                        return true;
                    });
                    filter.value = true;
                }
                // rm self loops
                if (inDegree[node] == 1 && outDegree[node] == 1) {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        if (sourceNodeId == targetNodeId) {
                            filter.value = true;
                            return false;
                        }
                        return false;
                    });
                }
                // remove
                if (filter.value) {
                    it.remove();
                }
                changes |= filter.value;
            }

        } while (changes && complete);
    }

    /**
     * find node with highest product of in- and out-degree
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
     * find all reachable nodes
     * @param set the nodeSet
     * @param pivotNodeId
     * @param direction
     * @return
     */
    public IntSet reach(IntSet set, int pivotNodeId, Direction direction) {
        final IntScatterSet ret = new IntScatterSet();
        final IntStack queue = new IntStack();
        queue.push(pivotNodeId);
        ret.add(pivotNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            if (!set.contains(node)) {
                continue;
            }
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                if (!ret.add(targetNodeId)) {
                    queue.push(targetNodeId);
                }
                return true;
            });
        }
        return ret;
    }

    private

    private static class Flag {
        public boolean value;
    }
}
