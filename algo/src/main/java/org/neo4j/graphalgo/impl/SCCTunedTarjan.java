package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Sequential strongly connected components algorithm (TunedTarjan).
 *
 * as specified in: https://pdfs.semanticscholar.org/61db/6892a92d1d5bdc83e52cc18041613cf895fa.pdf
 */
public class SCCTunedTarjan {

    private final Graph graph;
    private final IntStack edgeStack;

    private final IntStack open;
    private final int[] connectedComponents;
    private final int nodeCount;

    private int dfs = 0;
    private int scc = 0;

    public SCCTunedTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        connectedComponents = new int[nodeCount];
        edgeStack = new IntStack();
        open = new IntStack();
    }

    public SCCTunedTarjan compute() {
        dfs = -(nodeCount + 1);
        scc = 0;
        Arrays.fill(connectedComponents, -1);
        graph.forEachNode(node -> {
            if (connectedComponents[node] == -1) {
                lowPointDFS(node);
            }
            return true;
        });
        return this;
    }

    public int[] getConnectedComponents() {
        return connectedComponents;
    }

    public Stream<SCCStreamResult> resultStream() {
        return IntStream.range(0, nodeCount)
                .filter(i -> connectedComponents[i] != -1)
                .mapToObj(i -> new SCCStreamResult(graph.toOriginalNodeId(i), connectedComponents[i]));
    }

    private int lowPointDFS(int nodeId) {

        final int dfsNum = dfs++;
        connectedComponents[nodeId] = dfsNum;
        int lowPoint = dfsNum;
        open.push(nodeId);

        final int sz = edgeStack.size();
        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
            edgeStack.push(targetNodeId);
            return true;
        });

        while (edgeStack.size() > sz) {
            int w = edgeStack.pop();
            int d = connectedComponents[w];
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
                connectedComponents[u] = scc;
            } while (u != nodeId);
            scc++;
        }
        return lowPoint;
    }
}
