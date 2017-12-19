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

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
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
public class HugeParallelUnionFindQueue extends GraphUnionFindAlgo<HugeGraph, PagedDisjointSetStruct, HugeParallelUnionFindQueue> {

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
    public PagedDisjointSetStruct compute() {
        final List<Future<?>> futures = new ArrayList<>(stepSize);
        final BlockingQueue<PagedDisjointSetStruct> queue = new ArrayBlockingQueue<>(stepSize);

        int steps = 0;
        for (long i = 0L; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new HugeUnionFindTask(queue, i)));
            ++steps;
        }

        for (int i = 1; i < steps; ++i) {
            futures.add(executor.submit(() -> {
                try {
                    final PagedDisjointSetStruct a = queue.take();
                    final PagedDisjointSetStruct b = queue.take();
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

    public PagedDisjointSetStruct compute(double threshold) {
        throw new IllegalArgumentException("Not yet implemented");
    }

    private void await(final List<Future<?>> futures) {
        ParallelUtil.awaitTermination(futures);
    }

    private PagedDisjointSetStruct getStruct(final BlockingQueue<PagedDisjointSetStruct> queue) {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private class HugeUnionFindTask implements Runnable {

        private final HugeRelationshipIterator rels;
        private final BlockingQueue<PagedDisjointSetStruct> queue;
        private final long offset;
        private final long end;

        HugeUnionFindTask(BlockingQueue<PagedDisjointSetStruct> queue, long offset) {
            this.rels = graph.concurrentCopy();
            this.queue = queue;
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        public void run() {
            final PagedDisjointSetStruct struct = new PagedDisjointSetStruct(
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
