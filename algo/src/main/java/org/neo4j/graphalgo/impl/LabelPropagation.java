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

import com.carrotsearch.hppc.DoubleDoubleHashMap;
import com.carrotsearch.hppc.IntDoubleHashMap;
import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.cursors.DoubleDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class LabelPropagation extends Algorithm<LabelPropagation> {

    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final int[] EMPTY_INTS = new int[0];

    private HeavyGraph graph;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final int nodeCount;
    private Direction direction;

    public LabelPropagation(
            HeavyGraph graph,
            int batchSize,
            int concurrency,
            ExecutorService executor) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, batchSize);
        this.concurrency = concurrency;
        this.executor = executor;
    }

    public IntDoubleMap compute(
            Direction direction,
            long times) {
        if (times <= 0) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }
        this.direction = direction;

        final Collection<ComputeStep> computeSteps = ParallelUtil.readParallel(
                concurrency,
                batchSize,
                graph,
                (offset, nodes) -> new ComputeStep(batchSize, nodes),
                executor);

        for (long i = 1; i < times; i++) {
            ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        }
        final IntDoubleMap labels = new IntDoubleHashMap(nodeCount);
        for (ComputeStep computeStep : computeSteps) {
            labels.putAll(computeStep.labels);
            computeStep.release();
        }

        return labels;
    }

    @Override
    public LabelPropagation me() {
        return this;
    }

    @Override
    public LabelPropagation release() {
        graph = null;
        return this;
    }

    private final class ComputeStep implements Runnable, RelationshipConsumer {
        private final PrimitiveIntIterable nodes;
        private final DoubleDoubleHashMap votes;
        private final IntDoubleHashMap labels;
        private ProgressLogger progressLogger;

        private ComputeStep(int nodeSize, PrimitiveIntIterable nodes) {
            this.nodes = nodes;
            votes = new DoubleDoubleHashMap();
            labels = new IntDoubleHashMap(nodeSize);
            progressLogger = getProgressLogger();
        }

        @Override
        public void run() {
            PrimitiveIntIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                compute(iterator.next());
            }
        }

        private void compute(int nodeId) {
            votes.clear();
            graph.forEachRelationship(nodeId, direction, this);
            double originalPartition = partition(nodeId);
            double partition = originalPartition;
            double weight = Double.NEGATIVE_INFINITY;
            for (DoubleDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    partition = vote.key;
                }
            }
            if (partition != originalPartition) {
                labels.put(nodeId, partition);
            }
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        }

        @Override
        public boolean accept(
                final int sourceNodeId,
                final int targetNodeId,
                final long relationId) {
            double partition = partition(targetNodeId);
            double weight = graph.weightOf(sourceNodeId, targetNodeId) * graph.weightOf(targetNodeId);
            votes.addTo(partition, weight);
            return true;
        }

        private double partition(int node) {
            double partition = labels.getOrDefault(node, Double.NEGATIVE_INFINITY);
            return partition == Double.NEGATIVE_INFINITY ? graph.valueOf(node, node) : partition;
        }

        private void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

            votes.keys = EMPTY_DOUBLES;
            votes.clear();
            votes.keys = null;
            votes.values = null;

            labels.keys = EMPTY_INTS;
            labels.clear();
            labels.keys = null;
            labels.values = null;
        }
    }
}
