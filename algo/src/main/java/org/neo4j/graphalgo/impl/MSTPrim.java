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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Sequential Single-Source minimum weight spanning tree algorithm (PRIM).
 * <p>
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of {@link UndirectedTree}.
 * <p>
 * The algorithm also computes the minimum, maximum and sum of all
 * weights in the MST.
 *
 * @author mknblch
 */
public class MSTPrim extends Algorithm<MSTPrim> {

    private IdMapping idMapping;
    private BothRelationshipIterator iterator;
    private RelationshipWeights weights;
    private MinimumSpanningTree minimumSpanningTree;

    public MSTPrim(IdMapping idMapping, BothRelationshipIterator iterator, RelationshipWeights weights) {
        this.idMapping = idMapping;
        this.iterator = iterator;
        this.weights = weights;
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     *
     * @param startNode the node to start the evaluation from
     * @return a container of the transitions in the minimum spanning tree
     */
    public MSTPrim compute(int startNode) {
        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final int nodeCount = Math.toIntExact(idMapping.nodeCount());
        final BitSet visited = new BitSet(nodeCount);
        minimumSpanningTree = new MinimumSpanningTree(nodeCount, startNode, weights);
        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        iterator.forEachRelationship(startNode, (sourceNodeId, targetNodeId, relationId) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weights.weightOf(sourceNodeId, targetNodeId));
            return true;
        });
        while (!queue.isEmpty() && running()) {
            // retrieve cheapest transition
            final long transition = queue.pop();
            final int nodeId = getTail(transition);
            if (visited.get(nodeId)) {
                continue;
            }
            visited.set(nodeId);
            // add to mst
            minimumSpanningTree.addRelationship(getHead(transition), nodeId);
            // add new candidates
            iterator.forEachRelationship(nodeId, (sourceNodeId, targetNodeId, relationId) -> {
                queue.add(combineIntInt(nodeId, targetNodeId), weights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
        }
        return this;
    }

    public MinimumSpanningTree getMinimumSpanningTree() {
        return minimumSpanningTree;
    }

    @Override
    public MSTPrim me() {
        return this;
    }

    @Override
    public MSTPrim release() {
        idMapping = null;
        iterator = null;
        weights = null;
        minimumSpanningTree = null;
        return null;
    }

    public static class MinimumSpanningTree extends UndirectedTree {

        private final int startNodeId;
        private final RelationshipWeights weights;

        /**
         * Creates a new Tree that can hold up to {@code capacity} nodes.
         *
         * @param capacity
         * @param weights
         */
        public MinimumSpanningTree(int capacity, int startNodeId, RelationshipWeights weights) {
            super(capacity);
            this.startNodeId = startNodeId;
            this.weights = weights;
        }

        public int getStartNodeId() {
            return startNodeId;
        }

        public void forEachBFS(RelationshipConsumer consumer) {
            super.forEachBFS(startNodeId, consumer);
        }

        public void forEachDFS(RelationshipConsumer consumer) {
            super.forEachDFS(startNodeId, consumer);
        }

        public Aggregator aggregate() {
            final Aggregator aggregator = new Aggregator(weights);
            forEachBFS(aggregator);
            return aggregator;
        }

        public static class Aggregator implements RelationshipConsumer {

            private final RelationshipWeights relationshipWeights;
            private double sum = 0.0;
            private double min = Double.MAX_VALUE;
            private double max = Double.MIN_VALUE;
            private int count;


            private Aggregator(RelationshipWeights relationshipWeights) {
                this.relationshipWeights = relationshipWeights;
            }

            @Override
            public boolean accept(int sourceNodeId, int targetNodeId, long relationId) {
                double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                if (weight < min) {
                    min = weight;
                }
                if (weight > max) {
                    max = weight;
                }
                count++;
                sum += weight;
                return true;
            }

            public double getSum() {
                return sum;
            }

            public double getMin() {
                return min;
            }

            public double getMax() {
                return max;
            }

            public int getCount() {
                return count;
            }
        }
    }
}
