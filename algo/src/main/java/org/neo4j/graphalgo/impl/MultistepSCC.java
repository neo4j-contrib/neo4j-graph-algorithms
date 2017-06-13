package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.Iterator;
import java.util.function.IntConsumer;

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

    public void compute() {

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
                    it.remove();
                }
                changes |= filter[0];
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
     * bfs for finding all reachable nodes of set starting from pivotNode
     *  @param set the nodeSet
     * @param startNodeId the start node id
     * @param direction the direction to traverse
     */
    private void bfs(IntSet set, int startNodeId, Direction direction, IntConsumer consumer) {
        final SimpleBitSet visited = new SimpleBitSet(set.size());
        final IntStack queue = new IntStack();
        queue.push(startNodeId);
        visited.put(startNodeId);
        while (!queue.isEmpty()) {
            final int node = queue.pop();
            if (!set.contains(node)) {
                continue;
            }
            consumer.accept(node);
            graph.forEachRelationship(node, direction, (sourceNodeId, targetNodeId, relationId) -> {
                // don't visit id's not contained in set
                if (!set.contains(targetNodeId)) {
                    return true;
                }
                // don't visit id's twice
                if (visited.contains(targetNodeId)) {
                    return true;
                }
                queue.push(targetNodeId);
                return true;
            });
        }
    }

    private IntSet sweep(IntSet set, Direction direction) {
        final IntScatterSet ret = new IntScatterSet();
        bfs(set, pivot(set), direction, ret::add);
        return ret;
    }

}
