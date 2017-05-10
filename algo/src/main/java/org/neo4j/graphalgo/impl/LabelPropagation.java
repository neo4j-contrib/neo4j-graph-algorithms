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
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class LabelPropagation {

    private static final double[] EMPTY_DOUBLES = new double[0];
    private static final int[] EMPTY_INTS = new int[0];

    private final HeavyGraph graph;
    private final ExecutorService executor;
    private Direction direction;

    public LabelPropagation(
            HeavyGraph graph,
            ExecutorService executor) {
        this.graph = graph;
        this.executor = executor;
    }

    public IntDoubleMap compute(
            Direction direction,
            long times,
            int batchSize) {
        if (times <= 0) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }
        this.direction = direction;

        Collection<ComputeStep> computeSteps = ParallelUtil.readParallel(
                    batchSize,
                    graph,
                    (offset, nodes) -> new ComputeStep(batchSize, nodes),
                    executor);

        for (long i = 1; i < times; i++) {
            ParallelUtil.run(computeSteps, executor);
        }
        IntDoubleMap labels = new IntDoubleHashMap(graph.nodeCount());
        for (ComputeStep computeStep : computeSteps) {
            labels.putAll(computeStep.labels);
            computeStep.release();
        }

        return labels;
    }

    private final class ComputeStep implements Runnable, RelationshipConsumer {
        private final PrimitiveIntIterable nodes;
        private final DoubleDoubleHashMap votes;
        private final IntDoubleHashMap labels;

        private ComputeStep(int nodeSize, PrimitiveIntIterable nodes) {
            this.nodes = nodes;
            votes = new DoubleDoubleHashMap();
            labels = new IntDoubleHashMap(nodeSize);
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
