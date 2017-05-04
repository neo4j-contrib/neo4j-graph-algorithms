package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;


public class SCCTarjan {

    private final Graph graph;
    private Aggregator aggregator;

    public SCCTarjan(Graph graph) {
        this.graph = graph;
        int nodeCount = graph.nodeCount();

        aggregator = new Aggregator(graph,
                new int[nodeCount],
                new int[nodeCount],
                new ObjectArrayList<>(),
                new BitSet(nodeCount),
                new IntStack(nodeCount));
    }

    public SCCTarjan compute() {
        aggregator.reset();
        graph.forEachNode(aggregator);
        return this;
    }

    public ObjectArrayList<IntSet> getConnectedComponents() {
        return aggregator.connectedComponents;
    }

    public long getMaxSetSize() {
        return graph.nodeCount() == 0 ? 0 : aggregator.maxSetSize;
    }

    public long getMinSetSize() {
        return graph.nodeCount() == 0 ? 0 : aggregator.minSetSize;
    }

    private static final class Aggregator implements IntPredicate, RelationshipConsumer {

        private final Graph graph;
        private final int[] indices;
        private final int[] lowLink;
        private final ObjectArrayList<IntSet> connectedComponents; // TODO find better container (maybe DisjointSetStruct)
        private final BitSet onStack;
        private final IntStack stack;
        private int index;
        private long minSetSize = Long.MAX_VALUE;
        private long maxSetSize = 0;

        private Aggregator(Graph graph, int[] indices, int[] lowLink, ObjectArrayList<IntSet> connectedComponents, BitSet onStack, IntStack stack) {
            this.graph = graph;
            this.indices = indices;
            this.lowLink = lowLink;
            this.connectedComponents = connectedComponents;
            this.onStack = onStack;
            this.stack = stack;
        }

        public void reset() {
            connectedComponents.clear();
            Arrays.fill(indices, -1);
            Arrays.fill(lowLink, -1);
            onStack.clear();
            stack.clear();
            index = 0;
            minSetSize = Long.MAX_VALUE;
            maxSetSize = 0;
        }

        private void strongConnect(int node) {
            lowLink[node] = index;
            indices[node] = index;
            index++;
            stack.push(node);
            onStack.set(node);
            graph.forEachRelationship(node, Direction.OUTGOING, this);
            if (indices[node] == lowLink[node]) {
                relax(node);
            }
        }

        private void relax(int nodeId) {
            IntHashSet connected = new IntHashSet();
            int w;
            do {
                w = stack.pop();
                onStack.clear(w);
                connected.add(w);
            } while (w != nodeId);
            connectedComponents.add(connected);
            int size = connected.size();
            if (size < minSetSize) {
                minSetSize = size;
            }
            if (size > maxSetSize) {
                maxSetSize = size;
            }
        }

        @Override
        public boolean accept(int source, int target, long edgeId) {
            if (indices[target] == -1) {
                strongConnect(target);
                lowLink[source] = Math.min(lowLink[source], lowLink[target]);
            } else if (onStack.get(target)) {
                lowLink[source] = Math.min(lowLink[source], indices[target]);
            }
            return true;
        }

        @Override
        public boolean test(int node) {
            if (indices[node] == -1) {
                strongConnect(node);
            }
            return true;
        }
    }
}
