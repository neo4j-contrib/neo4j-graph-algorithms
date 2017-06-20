package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

/**
 * MultiStep SCC trimming algorithm. Removes trivial non-strongly connected
 * components
 * @author mknblch
 */
public class MultiStepTrim {

    private final Graph graph;
    private final int nodeCount;
    private final IntSet nodes;

    // auxiliary arrays for nodeCounts
    private final int[] inDegree;
    private final int[] outDegree;

    public MultiStepTrim(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        nodes = new IntHashSet();
        inDegree = new int[nodeCount];
        outDegree = new int[nodeCount];
    }

    public IntSet compute(boolean complete) {
        reset();
        trim(complete);
        return nodes;
    }

    private void reset() {
        for (int i = nodeCount - 1; i >= 0; i--) {
            nodes.add(i);
            inDegree[i] = graph.degree(i, Direction.INCOMING);
            outDegree[i] = graph.degree(i, Direction.OUTGOING);
        }
    }

    /**
     * trim the graph by removing nodes with IN- or OUT-Degree of 0
     * and all self-referencing nodes without relationships to others.
     *
     * @param complete true: prune the graph until no more changes can be made
     *                 does only one iteration otherwise
     */
    private void trim(boolean complete) {
        final IntScatterSet remove = new IntScatterSet();
        boolean changes; // tells whether the last iteration changed the graph
        final boolean[] filter = {false};
        do {
            changes = false;
            for(Iterator<IntCursor> it = nodes.iterator(); it.hasNext(); ) {
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
                if (outDegree[node] == 0) {
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
            nodes.removeAll(remove);
        } while (changes && complete);
    }
}
