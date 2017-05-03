package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Impl. Prim's Minimum Weight Spanning Tree
 *
 * @author mknobloch
 */
public class MSTPrim {

    private final IdMapping idMapping;
    private final BothRelationshipIterator iterator;
    private final RelationshipWeights weights;
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
        final BitSet visited = new BitSet(idMapping.nodeCount());
        minimumSpanningTree = new MinimumSpanningTree(idMapping.nodeCount(), startNode, weights);
        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        iterator.forEachRelationship(startNode, (sourceNodeId, targetNodeId, relationId) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weights.weightOf(sourceNodeId, targetNodeId));
            return true;
        });
        while (!queue.isEmpty()) {
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
