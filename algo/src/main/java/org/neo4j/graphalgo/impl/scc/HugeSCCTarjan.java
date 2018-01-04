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

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.*;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

/**
 * Sequential strongly connected components algorithm (Tarjan).
 * <p>
 * Builds sets of node-Ids which represent a strongly connected component
 * within the graph. Also calculates minimum and maximum setSize as well
 * as the overall count of distinct sets.
 */
public class HugeSCCTarjan extends Algorithm<HugeSCCTarjan> {

    private HugeGraph graph;
    private final AllocationTracker allocationTracker;
    private final long nodeCount;

    private Aggregator aggregator;

    public HugeSCCTarjan(HugeGraph graph, AllocationTracker allocationTracker) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.allocationTracker = allocationTracker;

        new Aggregator(
                graph,
                LongArray.newArray(nodeCount, allocationTracker),
                LongArray.newArray(nodeCount, allocationTracker),
                new ObjectArrayList<>(),
                PagedSimpleBitSet.newBitSet(nodeCount, allocationTracker),
                new PagedLongStack(nodeCount, allocationTracker));
    }

    public HugeSCCTarjan compute() {
        aggregator.reset();
        graph.forEachNode(aggregator::compute);
        return this;
    }

    /**
     * get connected components list
     *
     * @return list of sets of strongly connected component ID's
     */
    public ObjectArrayList<LongSet> getConnectedComponents() {
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
    public HugeSCCTarjan me() {
        return this;
    }

    @Override
    public HugeSCCTarjan release() {
        aggregator = null;
        graph = null;
        return this;
    }


    private final class Aggregator {

        private final HugeGraph graph;
        private final LongArray indices;
        private final LongArray lowLink;
        private final ObjectArrayList<LongSet> connectedComponents;
        private final PagedSimpleBitSet onStack;
        private final PagedLongStack stack;

        private long index;
        private long minSetSize = Long.MAX_VALUE;
        private long maxSetSize = 0;
        private ProgressLogger progressLogger;

        private Aggregator(HugeGraph graph, LongArray indices, LongArray lowLink, ObjectArrayList<LongSet> connectedComponents, PagedSimpleBitSet onStack, PagedLongStack stack) {
            this.graph = graph;
            this.indices = indices;
            this.lowLink = lowLink;
            this.connectedComponents = connectedComponents;
            this.onStack = onStack;
            this.stack = stack;
        }

        public void reset() {
            connectedComponents.clear();
            indices.fill(-1);
            lowLink.fill(-1);
            onStack.clear();
            stack.clear();
            index = 0;
            minSetSize = Long.MAX_VALUE;
            maxSetSize = 0;
        }

        private void strongConnect(long node) {
            lowLink.set(node, index);
            indices.set(node, index);
            index++;
            stack.push(node);
            onStack.put(node);
            graph.forEachRelationship(node, Direction.OUTGOING, ((s, t) -> {
                if (indices.get(t) == -1L) {
                    strongConnect(t);
                    lowLink.set(s, Math.min(lowLink.get(s), lowLink.get(t)));
                } else if (onStack.contains(t)) {
                    lowLink.set(s, Math.min(lowLink.get(s), indices.get(t)));
                }
                return true;
            }));

            if (indices.get(node) == lowLink.get(node)) {
                relax(node);
            }
        }

        private void relax(long nodeId) {
            LongHashSet connected = new LongHashSet();
            long w;
            do {
                w = stack.pop();
                onStack.remove(w);
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

        public boolean compute(int node) {
            if (indices.get(node) == -1L) {
                strongConnect(node);
            }
            progressLogger.logProgress((double) node / (nodeCount - 1));
            return running();
        }
    }
}
