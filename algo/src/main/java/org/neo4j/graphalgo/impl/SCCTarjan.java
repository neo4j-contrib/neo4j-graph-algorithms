package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntPredicate;

/**
 * Sequential strongly connected components algorithm (Tarjan).
 *
 * Builds sets of node-Ids which represent a strongly connected component
 * within the graph. Also calculates minimum and maximum setSize as well
 * as the overall count of distinct sets.
 *
 */
public class SCCTarjan {

    private final Graph graph;
    private final int nodeCount;

    private int dfsCount = 0;
    private int sccCount = 0;

    private final int[] compNumArray;
    private final IntStack edgeStack;
    private final IntStack open;

    public SCCTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        indices = new int[nodeCount];
        lowLink = new int[nodeCount];
        connectedComponents = new ObjectArrayList<>();
        onStack = new SimpleBitSet(nodeCount);
        stack = new IntStack();
        compNumArray = new int[nodeCount];

    }

    public SCCTarjan compute() {
        reset();

        dfsCount = -(nodeCount + 1);
        sccCount = 0;
        Arrays.fill(compNumArray);


        return this;
    }

    private void reset() {
    }

    private int lowPointDFS(int nodeId) {

        final int dfsNum = dfsCount++;
        compNumArray[nodeId] = dfsNum;
        open.push();
        final int sz = edgeStack.size();
        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
            edgeStack.push(targetNodeId);
            return true;
        });
        int lowPoint = dfsNum;
        while (edgeStack.size() > sz) {
            int w = edgeStack.pop();
            int d = compNumArray[w];
            if (d == -1) {
                d = lowPointDFS(w);
            }
            if (d < lowPoint) {
                lowPoint = d;
            }
        }
        if (dfsNum == lowPoint) {
            int u;
            do {
                u = open.pop();
                compNumArray[u] = sccCount;
            } while (u != nodeId);
        }
        return lowPoint;
    }
}
