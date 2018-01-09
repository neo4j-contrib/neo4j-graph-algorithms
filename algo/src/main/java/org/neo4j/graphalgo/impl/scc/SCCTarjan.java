/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.scc;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.ObjectArrayList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;
import java.util.function.IntPredicate;

/**
 * Sequential strongly connected components algorithm (Tarjan).
 * <p>
 * Builds sets of node-Ids which represent a strongly connected component
 * within the graph. Also calculates minimum and maximum setSize as well
 * as the overall count of distinct sets.
 */
public class SCCTarjan extends Algorithm<SCCTarjan> {

    private Graph graph;
    private final int nodeCount;
    private Aggregator aggregator;

    public SCCTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());

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

    /**
     * get connected components list
     *
     * @return list of sets of strongly connected component ID's
     */
    public ObjectArrayList<IntSet> getConnectedComponents() {
        return aggregator.connectedComponents;
    }

    /**
     * return the maximum set size
     *
     * @return the maximum set size
     */
    public long getMaxSetSize() {
        return graph.nodeCount() == 0 ? 0 : aggregator.maxSetSize;
    }

    /**
     * return the minimum set size
     *
     * @return minimum set size
     */
    public long getMinSetSize() {
        return graph.nodeCount() == 0 ? 0 : aggregator.minSetSize;
    }

    @Override
    public SCCTarjan me() {
        return this;
    }

    @Override
    public SCCTarjan release() {
        aggregator = null;
        graph = null;
        return this;
    }

    private final class Aggregator implements IntPredicate, RelationshipConsumer {

        private final Graph graph;
        private final int[] indices;
        private final int[] lowLink;
        private final ObjectArrayList<IntSet> connectedComponents;
        private final BitSet onStack;
        private final IntStack stack;
        private int index;
        private long minSetSize = Long.MAX_VALUE;
        private long maxSetSize = 0;
        private ProgressLogger progressLogger;

        private Aggregator(Graph graph, int[] indices, int[] lowLink, ObjectArrayList<IntSet> connectedComponents, BitSet onStack, IntStack stack) {
            this.graph = graph;
            this.indices = indices;
            this.lowLink = lowLink;
            this.connectedComponents = connectedComponents;
            this.onStack = onStack;
            this.stack = stack;
            this.progressLogger = getProgressLogger();
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
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return running();
        }
    }
}
