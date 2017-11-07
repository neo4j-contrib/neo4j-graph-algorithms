package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * parallel UnionFind using ExecutorService only.
 * <p>
 * Algorithm based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The implementation is based on a queue which acts as a buffer
 * for each computed DSS. As long as there are more elements on
 * the queue the algorithm takes two, merges them and adds its
 * result to the queue until only 1 element remains.
 *
 * @author mknblch
 */
public class HugeParallelUnionFindQueue extends GraphUnionFindAlgo<HugeGraph, HugeDisjointSetStruct, HugeParallelUnionFindQueue> {

    private final ExecutorService executor;
    private final long nodeCount;
    private final long batchSize;
    private final int stepSize;
    private final AllocationTracker tracker;

    /**
     * initialize parallel UF
     */
    HugeParallelUnionFindQueue(
            HugeGraph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            AllocationTracker tracker) {
        super(graph);
        this.executor = executor;
        nodeCount = graph.nodeCount();
        this.tracker = tracker;
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize,
                Integer.MAX_VALUE);

        long targetSteps = ParallelUtil.threadSize(batchSize, nodeCount);
        if (targetSteps > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                    nodeCount,
                    concurrency,
                    batchSize));
        }
        stepSize = (int) targetSteps;
    }

    @Override
    public HugeDisjointSetStruct compute() {
        final List<Future<?>> futures = new ArrayList<>(stepSize);
        final BlockingQueue<HugeDisjointSetStruct> queue = new ArrayBlockingQueue<>(stepSize);

        int steps = 0;
        for (long i = 0L; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new HugeUnionFindTask(queue, i)));
            ++steps;
        }

        for (int i = 1; i < steps; ++i) {
            futures.add(executor.submit(() -> {
                try {
                    final HugeDisjointSetStruct a = queue.take();
                    final HugeDisjointSetStruct b = queue.take();
                    queue.add(a.merge(b));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }));
        }

        await(futures);
        return getStruct(queue);
    }

    public HugeDisjointSetStruct compute(double threshold) {
        throw new IllegalArgumentException("Not yet implemented");
    }

    private void await(final List<Future<?>> futures) {
        ParallelUtil.awaitTermination(futures);
    }

    private HugeDisjointSetStruct getStruct(final BlockingQueue<HugeDisjointSetStruct> queue) {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private class HugeUnionFindTask implements Runnable {

        private final HugeRelationshipIterator rels;
        private final BlockingQueue<HugeDisjointSetStruct> queue;
        private final long offset;
        private final long end;

        HugeUnionFindTask(BlockingQueue<HugeDisjointSetStruct> queue, long offset) {
            this.rels = graph.concurrentCopy();
            this.queue = queue;
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        public void run() {
            final HugeDisjointSetStruct struct = new HugeDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end; node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            struct.union(sourceNodeId, targetNodeId);
                            return true;
                        });
            }
            getProgressLogger().logProgress((end - 1.0) / (nodeCount - 1.0));
            try {
                queue.put(struct);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
