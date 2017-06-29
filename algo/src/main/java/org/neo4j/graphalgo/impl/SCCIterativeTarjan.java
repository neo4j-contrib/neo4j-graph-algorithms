package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * non recursive sequential strongly connected components algorithm.
 *
 * as specified in:  http://code.activestate.com/recipes/578507-strongly-connected-components-of-a-directed-graph/
 */
public class SCCIterativeTarjan {

    private enum Action {
        VISIT(0),
        VISITEDGE(1),
        POSTVISIT(2);

        public final int code;

        Action(int code) {
            this.code = code;
        }
    }

    private final Graph graph;
    private final int nodeCount;
    private final int[] index;
    private final SimpleBitSet visited;
    private final int[] connectedComponents;
    private final IntStack stack;
    private final IntStack boundaries;
    private final IntStack todo;

    public SCCIterativeTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        index = new int[nodeCount];
        stack = new IntStack();
        boundaries = new IntStack();
        connectedComponents = new int[nodeCount];
        visited = new SimpleBitSet(nodeCount);
        todo = new IntStack();
    }

    public SCCIterativeTarjan compute() {
        Arrays.fill(index, -1);
        Arrays.fill(connectedComponents, -1);
        todo.clear();
        boundaries.clear();
        stack.clear();
        graph.forEachNode(this::compute);
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

    private boolean compute(int nodeId) {
        if (index[nodeId] != -1) {
            return true;
        }
        push(Action.VISIT, nodeId);
        while (!todo.isEmpty()) {
            final int action = todo.pop();
            final int node = todo.pop();
            if (action == Action.VISIT.code) {
                visit(node);
            } else if (action == Action.VISITEDGE.code) {
                visitEdge(node);
            } else {
                postVisit(node);
            }
        }
        return true;
    }

    private void visitEdge(int node) {
        if (index[node] == -1) {
            push(Action.VISIT, node);
        } else if (!visited.contains(node)){
            while (index[node] < boundaries.peek()) {
                boundaries.pop();
            }
        }
    }

    private void postVisit(int nodeId) {
        if (boundaries.peek() == index[nodeId]) {
            boundaries.pop();
            int w, k = -1;
            do {
                w = stack.pop();
                if (k == -1) {
                    k = w;
                }
                connectedComponents[w] = k;
                visited.put(w);
            } while (w != nodeId);
        }
    }

    private void visit(int nodeId) {
        final int stackSize = stack.size();
        index[nodeId] = stackSize;
        stack.push(nodeId);
        boundaries.push(stackSize);
        push(Action.POSTVISIT, nodeId);
        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
            push(Action.VISITEDGE, targetNodeId);
            return true;
        });
    }

    /**
     * pushes an action and a nodeId on the stack
     * @param action
     * @param value
     */
    private void push(Action action, int value) {
        todo.push(value);
        todo.push(action.code);
    }
}
