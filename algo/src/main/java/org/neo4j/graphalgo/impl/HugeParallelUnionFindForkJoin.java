package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * parallel UnionFind using common ForkJoin-Pool only.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The UnionFindTask extracts a nodePartition if its input-set is too big and
 * calculates its result while lending the rest-nodeSet to another FJ-Task.
 * <p>
 * Note: The splitting method might be sub-optimal since the resulting work-tree is
 * very unbalanced so each thread needs to wait for its predecessor to complete
 * before merging his set into the parent set.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindForkJoin extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, HugeParallelUnionFindForkJoin> {

    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;

    /**
     * initialize parallel UF
     *
     * @param graph
     */
    HugeParallelUnionFindForkJoin(
            HugeGraph graph,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph);
        nodeCount = graph.nodeCount();
        this.tracker = tracker;
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);

    }

    public HugeDisjointSetStruct compute() {
        return ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
    }


    public HugeDisjointSetStruct compute(double threshold) {
        return ForkJoinPool
                .commonPool()
                .invoke(new ThresholdUFTask(0, threshold));
    }

    private class UnionFindTask extends RecursiveTask<HugeDisjointSetStruct> {

        private final long offset;
        private final long end;
        private final HugeRelationshipIterator rels;

        UnionFindTask(long offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            this.rels = graph.concurrentCopy();
        }

        @Override
        protected HugeDisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected HugeDisjointSetStruct run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            struct.union(sourceNodeId, targetNodeId);
                            return true;
                        });
            }
            getProgressLogger().logProgress(end - 1, nodeCount - 1);

            return struct;
        }
    }

    private class ThresholdUFTask extends RecursiveTask<HugeDisjointSetStruct> {

        private final long offset;
        private final long end;
        private final HugeRelationshipIterator rels;
        private final double threshold;

        ThresholdUFTask(long offset, double threshold) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            this.rels = graph.concurrentCopy();
            this.threshold = threshold;
        }

        @Override
        protected HugeDisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final ThresholdUFTask process = new ThresholdUFTask(
                        offset,
                        end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected HugeDisjointSetStruct run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (source, target) -> {
                            double weight = graph.weightOf(source, target);
                            if (weight >= threshold && !struct.connected(
                                    source,
                                    target)) {
                                struct.union(source, target);
                            }
                            return true;
                        });
            }
            return struct;
        }
    }
}
