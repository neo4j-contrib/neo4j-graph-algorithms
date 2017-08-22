package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Sequential strongly connected components algorithm (TunedTarjan).
 * <p>
 * as specified in: https://pdfs.semanticscholar.org/61db/6892a92d1d5bdc83e52cc18041613cf895fa.pdf
 */
public class SCCTunedTarjan extends Algorithm<SCCTunedTarjan> {

    private Graph graph;
    private IntStack edgeStack;

    private IntStack open;
    private int[] connectedComponents;
    private int nodeCount;

    private int dfs = 0;
    private int setCount = 0;
    private int minSetSize = Integer.MAX_VALUE;
    private int maxSetSize = 0;

    public SCCTunedTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        connectedComponents = new int[nodeCount];
        edgeStack = new IntStack();
        open = new IntStack();
    }

    public SCCTunedTarjan compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        Arrays.fill(connectedComponents, -1);
        setCount = 0;
        dfs = 0;
        setCount = 0;
        maxSetSize = 0;
        minSetSize = Integer.MAX_VALUE;
        dfs = -(nodeCount + 1);
        graph.forEachNode(node -> {
            if (connectedComponents[node] == -1) {
                lowPointDFS(node);
            }
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return running();
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

    public int getSetCount() {
        return setCount;
    }

    public int getMinSetSize() {
        return minSetSize;
    }

    public int getMaxSetSize() {
        return maxSetSize;
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
            int elementCount = 0;
            int element;
            do {
                element = open.pop();
                connectedComponents[element] = setCount;
                elementCount++;
            } while (element != nodeId);
            this.minSetSize = Math.min(this.minSetSize, elementCount);
            this.maxSetSize = Math.max(this.maxSetSize, elementCount);
            setCount++;
        }
        return lowPoint;
    }

    @Override
    public SCCTunedTarjan me() {
        return this;
    }

    @Override
    public SCCTunedTarjan release() {
        graph = null;
        edgeStack = null;
        open = null;
        connectedComponents = null;
        return this;
    }
}
