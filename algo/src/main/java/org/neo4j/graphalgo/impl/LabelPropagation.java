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

import com.carrotsearch.hppc.IntDoubleHashMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.concurrent.ExecutorService;

public final class LabelPropagation extends BaseLabelPropagation<
        Graph,
        WeightMapping,
        LabelPropagationAlgorithm.LabelArray,
        LabelPropagation> {

    public LabelPropagation(
            Graph graph,
            NodeProperties nodeProperties,
            int batchSize,
            int concurrency,
            ExecutorService executor) {
        super(graph,
                nodeProperties.nodeProperties(PARTITION_TYPE),
                nodeProperties.nodeProperties(WEIGHT_TYPE),
                batchSize,
                concurrency,
                executor,
                AllocationTracker.EMPTY);
    }

    @Override
    LabelArray initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new LabelArray(new int[Math.toIntExact(nodeCount)]);
    }

    @Override
    List<BaseStep> baseSteps(
            final Graph graph,
            final LabelArray labels,
            final WeightMapping nodeProperties,
            final WeightMapping nodeWeights,
            final Direction direction,
            final boolean randomizeOrder) {
        return ParallelUtil.readParallel(
                concurrency,
                batchSize,
                graph,
                (offset, nodes) -> {
                    InitStep initStep = new InitStep(
                            graph,
                            labels.labels,
                            direction,
                            randomizeOrder,
                            getProgressLogger(),
                            nodes,
                            nodeProperties,
                            nodeWeights
                    );
                    return asStep(initStep);
                },
                executor);

    }

    private static final class InitStep extends Initialization {

        private final Graph graph;
        private final int[] existingLabels;
        private final Direction direction;
        private final boolean randomizeOrder;
        private final ProgressLogger progressLogger;
        private final PrimitiveIntIterable nodes;
        private final WeightMapping nodeProperties;
        private final WeightMapping nodeWeights;

        private InitStep(
                Graph graph,
                int[] existingLabels,
                Direction direction,
                boolean randomizeOrder,
                ProgressLogger progressLogger,
                PrimitiveIntIterable nodes,
                WeightMapping nodeProperties,
                WeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.randomizeOrder = randomizeOrder;
            this.progressLogger = progressLogger;
            this.nodes = nodes;
            this.nodeProperties = nodeProperties;
            this.nodeWeights = nodeWeights;
        }

        @Override
        void setExistingLabels() {
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                int nodeId = iterator.next();
                int existingLabel = (int) this.nodeProperties.get(nodeId, (double) nodeId);
                existingLabels[nodeId] = existingLabel;
            }
        }

        @Override
        Computation computeStep() {
            return new ComputeStep(
                    graph,
                    existingLabels,
                    direction,
                    randomizeOrder,
                    progressLogger,
                    nodes,
                    nodeWeights);
        }
    }

    private static final class ComputeStep extends Computation implements RelationshipConsumer {

        private final Graph graph;
        private final int[] existingLabels;
        private final Direction direction;
        private final ProgressLogger progressLogger;
        private final PrimitiveIntIterable nodes;
        private final int maxNode;
        private final IntDoubleHashMap votes;
        private final WeightMapping nodeWeights;

        private ComputeStep(
                Graph graph,
                int[] existingLabels,
                Direction direction,
                boolean randomizeOrder,
                ProgressLogger progressLogger,
                PrimitiveIntIterable nodes,
                WeightMapping nodeWeights) {
            this.graph = graph;
            this.existingLabels = existingLabels;
            this.direction = direction;
            this.progressLogger = progressLogger;
            this.nodes = RandomlySwitchingIntIterable.of(randomizeOrder, nodes);
            this.maxNode = (int) (graph.nodeCount() - 1L);
            this.votes = new IntDoubleScatterMap();
            this.nodeWeights = nodeWeights;
        }

        @Override
        boolean computeAll() {
            PrimitiveIntIterator iterator = nodes.iterator();
            boolean didChange = false;
            while (iterator.hasNext()) {
                didChange = compute(iterator.next(), didChange);
            }
            return didChange;
        }

        private boolean compute(int nodeId, boolean didChange) {
            votes.clear();
            int partition = existingLabels[nodeId];
            int previous = partition;
            graph.forEachRelationship(nodeId, direction, this);
            double weight = Double.NEGATIVE_INFINITY;
            for (IntDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    partition = vote.key;
                }
            }
            progressLogger.logProgress(nodeId, maxNode);
            if (partition != previous) {
                existingLabels[nodeId] = partition;
                return true;
            }
            return didChange;
        }

        @Override
        public boolean accept(
                final int sourceNodeId,
                final int targetNodeId,
                final long relationId) {
            int partition = existingLabels[targetNodeId];
            double weight = graph.weightOf(sourceNodeId, targetNodeId) * nodeWeights.get(targetNodeId);
            votes.addTo(partition, weight);
            return true;
        }

        @Override
        void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

            if (votes.keys != null) {
                votes.keys = EMPTY_INTS;
                votes.clear();
                votes.keys = null;
                votes.values = null;
            }
        }
    }
}
