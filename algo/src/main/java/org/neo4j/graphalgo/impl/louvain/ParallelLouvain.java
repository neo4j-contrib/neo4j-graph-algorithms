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


import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Parallel modularity based community detection algo
 *
 * @author mknblch
 */
public class ParallelLouvain extends Algorithm<ParallelLouvain> implements LouvainAlgorithm {

    private final ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private RelationshipIterator relationshipIterator;
    private IdMapping idMapping;
    private Degrees degrees;
    private final AtomicInteger queue;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    private final ArrayList<Task> tasks = new ArrayList<>();
    private double[] sTot;
    private double m2, mq2;

    private final int[] communityIds; // node to community mapping
    private int iterations;
    private final int maxIterations;

    public ParallelLouvain(IdMapping idMapping,
                           RelationshipIterator relationshipIterator,
                           Degrees degrees,
                           ExecutorService executorService,
                           int concurrency,
                           int maxIterations) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        communityIds = new int[nodeCount];
        sTot = new double[nodeCount];
        this.queue = new AtomicInteger(0);

    }

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
    public ParallelLouvain release() {
        relationshipIterator = null;
        return this;
    }

    private void reset() {

        tasks.clear();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }

        Arrays.setAll(communityIds, i -> i);

        final LongAdder adder = new LongAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            final int d = degrees.degree(node, Direction.OUTGOING);
            sTot[node] = d;
            adder.add(d);
        });
        final double allDegree = (double) adder.intValue();
        this.m2 = allDegree * 4.0; // 2m //
        mq2 = 2.0 * Math.pow(allDegree, 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) { // TODO sync
        final int d = degrees.degree(node, Direction.OUTGOING);

        writeLock.lock();
        try {
            sTot[communityIds[node]] -= d;
            sTot[targetCommunity] += d;
            // update communityIds
            communityIds[node] = targetCommunity;
        } finally {
            writeLock.unlock();
        }
    }

     /**
     * @return kiIn
     */
    private int kIIn(int node, int targetCommunity) {
        int[] sum = {0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[0]++;
            }
            return true;
        });

        return sum[0];
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(idMapping.toOriginalNodeId(i), communityIds[i]));
    }

    public int[] getCommunityIds() {
        return communityIds;
    }

    public int getIterations() {
        return iterations;
    }

    public long getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < communityIds.length; i++) {
            bitSet.put(communityIds[i]);
        }
        return bitSet.size();
    }

    @Override
    public ParallelLouvain me() {
        return this;
    }

    private class Task implements Runnable {

        private boolean changes = false;
        private double bestGain;
        private int bestCommunity;
        private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        private final TerminationFlag flag = getTerminationFlag();
        private final ProgressLogger logger = getProgressLogger();

        @Override
        public void run() {
            changes = false;
            for (int node; (node = queue.getAndIncrement()) < nodeCount && flag.running(); ) {
                bestGain = 0.0;
                readLock.lock();
                final int sourceCommunity = bestCommunity = communityIds[node];
                final double mSource = (sTot[sourceCommunity] * degrees.degree(node, Direction.OUTGOING)) / mq2;
                readLock.unlock();

                relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                    readLock.lock();
                    final int targetCommunity = communityIds[targetNodeId];
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
