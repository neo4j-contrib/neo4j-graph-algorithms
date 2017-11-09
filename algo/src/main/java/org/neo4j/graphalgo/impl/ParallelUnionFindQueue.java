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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

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
public class ParallelUnionFindQueue extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ParallelUnionFindQueue> {

    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private final int stepSize;

    public static Function<Graph, ParallelUnionFindQueue> of(ExecutorService executor, int minBatchSize, int concurrency) {
        return graph -> new ParallelUnionFindQueue(
                graph,
                executor,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize parallel UF
     */
    public ParallelUnionFindQueue(Graph graph, ExecutorService executor, int minBatchSize, int concurrency) {
        super(graph);
        this.executor = executor;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, minBatchSize);
        stepSize = ParallelUtil.threadSize(batchSize, nodeCount);
    }

    @Override
    public DisjointSetStruct compute() {
        final List<Future<?>> futures = new ArrayList<>(stepSize);
        final BlockingQueue<DisjointSetStruct> queue = new ArrayBlockingQueue<>(stepSize);

        Phaser phaser = new Phaser();
        int steps = 0;
        for (int i = 0; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new UnionFindTask(queue, i, phaser)));
            ++steps;
        }
        phaser.awaitAdvance(phaser.getPhase());

        for (int i = 1; i < steps; ++i) {
            futures.add(executor.submit(() -> {
                try {
                    final DisjointSetStruct a = queue.take();
                    final DisjointSetStruct b = queue.take();
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

    private void await(final List<Future<?>> futures) {
        ParallelUtil.awaitTermination(futures);
    }

    @Override
    public DisjointSetStruct compute(double threshold) {
        throw new IllegalArgumentException("Parallel UnionFind with threshold not implemented, please use either `concurrency:1` or one of the exp* variants of UnionFind");
    }

    private DisjointSetStruct getStruct(final BlockingQueue<DisjointSetStruct> queue) {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private class UnionFindTask implements Runnable {

        private final BlockingQueue<DisjointSetStruct> queue;
        private final Phaser phaser;
        private final int offset;
        private final int end;

        UnionFindTask(
                BlockingQueue<DisjointSetStruct> queue,
                int offset,
                Phaser phaser) {
            this.queue = queue;
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            this.phaser = phaser;
            phaser.register();
        }

        @Override
        public void run() {
            phaser.arriveAndDeregister();
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end; node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
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
