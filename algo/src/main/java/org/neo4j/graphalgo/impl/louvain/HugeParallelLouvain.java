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
package org.neo4j.graphalgo.impl.louvain;


import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.PagedSimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Parallel modularity based community detection algo
 *
 * @author mknblch
 */
public class HugeParallelLouvain extends Algorithm<HugeParallelLouvain> implements LouvainAlgorithm {

    /**
     * pool
     */
    private ExecutorService executorService;
    /**
     * number of threads to use
     */
    private final int concurrency;
    /**
     * node cound
     */
    private final long nodeCount;
    /**
     * graph
     */
    private HugeGraph graph;
    /**
     * incrementing node counter
     */
    private final AtomicLong queue;
    /**
     * R&W Locks
     */
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    /**
     * task array for parallel execution
     */
    private final ArrayList<Task> tasks = new ArrayList<>();
    /**
     * memory tracker
     */
    private final AllocationTracker tracker;
    /**
     * community weight. Sum of degrees of nodes
     * within a cluster
     */
    private DoubleArray communityWeights;
    /**
     * pre calculated values
     */
    private double m2, mq2;
    /**
     * node to community id mapping
     */
    private HugeLongArray communityIds;
    /**
     * number of iterations so far
     */
    private int iterations;
    /**
     * maximum number of iterations
     */
    private final int maxIterations;

    public HugeParallelLouvain(HugeGraph graph,
                               ExecutorService executorService,
                               AllocationTracker tracker,
                               int concurrency,
                               int maxIterations) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        communityIds = HugeLongArray.newArray(nodeCount, tracker);
        communityWeights = DoubleArray.newArray(nodeCount, tracker);
        this.queue = new AtomicLong(0);
        this.tracker = tracker;

    }

    /**
     * cluster id's until either max iterations is reached or no further
     * changes could improve modularity
     */
    public LouvainAlgorithm compute() {
        reset();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            queue.set(0);
            ParallelUtil.runWithConcurrency(concurrency, tasks, getTerminationFlag(), executorService);
            boolean changes = false;
            for (Task task : tasks) {
                changes |= task.changes;
            }
            if (!changes) {
                return this;
            }
        }
        return this;
    }

    @Override
    public HugeParallelLouvain release() {
        graph = null;
        executorService = null;
        communityIds = null;
        communityWeights = null;
        return this;
    }

    private void reset() {

        tasks.clear();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }

        communityIds.setAll(i -> i);
        final LongAdder adder = new LongAdder();
        ParallelUtil.iterateParallelHuge(executorService, nodeCount, concurrency, node -> {
            final int d = graph.degree(node, Direction.OUTGOING);
            communityWeights.set(node, d);
            adder.add(d);
        });
        /**
         * we can iterate over outgoing rels only because
         * the graph should be treated as undirected and
         * therefore have to multiply m by 4 instead of 2
         */
        m2 = adder.intValue() * 4.0; // 2m
        mq2 = 2.0 * Math.pow(adder.intValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(long node, long targetCommunity) {
        final int d = graph.degree(node, Direction.OUTGOING);
        writeLock.lock();
        try {
            final long index = communityIds.get(node);
            communityWeights.add(index, -d);
            communityWeights.add(targetCommunity, d);
            // update communityIds
            communityIds.set(node, targetCommunity);
        } finally {
            writeLock.unlock();
        }
    }

     /**
     * @return kiIn
     */
    private int kIIn(long node, long targetCommunity) {
        int[] sum = {0}; // {ki, ki_in}
        graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
            if (targetCommunity == communityIds.get(targetNodeId)) {
                sum[0]++;
            }
            return true;
        });

        return sum[0];
    }

    public Stream<Result> resultStream() {
        return LongStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(graph.toOriginalNodeId(i), communityIds.get(i)));
    }

    public HugeLongArray getCommunityIds() {
        return communityIds;
    }

    public int getIterations() {
        return iterations;
    }

    public long getCommunityCount() {
        final PagedSimpleBitSet bitSet = PagedSimpleBitSet.newBitSet(nodeCount, tracker);
        for (long i = 0; i < communityIds.size(); i++) {
            bitSet.put(communityIds.get(i));
        }
        return bitSet._size();
    }

    @Override
    public HugeParallelLouvain me() {
        return this;
    }

    private class Task implements Runnable {

        private boolean changes = false;
        private double bestGain;
        private long bestCommunity;
        private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        private final TerminationFlag flag = getTerminationFlag();
        private final ProgressLogger logger = getProgressLogger();

        @Override
        public void run() {
            changes = false;
            for (long node; (node = queue.getAndIncrement()) < nodeCount && flag.running(); ) {
                bestGain = 0.0;
                readLock.lock();
                final long sourceCommunity = bestCommunity = communityIds.get(node);
                final double mSource = (communityWeights.get(sourceCommunity) * graph.degree(node, Direction.OUTGOING)) / mq2;
                readLock.unlock();

                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                    readLock.lock();
                    final long targetCommunity = communityIds.get(targetNodeId);
                    readLock.unlock();
                    final double gain = kIIn(sourceNodeId, targetCommunity) / m2 - mSource;
                    if (gain > bestGain) {
                        bestCommunity = targetCommunity;
                        bestGain = gain;
                    }
                    return flag.running();
                });
                if (bestCommunity != sourceCommunity) {
                    assign(node, bestCommunity);
                    changes = true;
                }
                logger.logProgress(node, nodeCount - 1);
            }
        }
    }

}
