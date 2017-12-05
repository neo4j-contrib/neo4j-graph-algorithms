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


import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ParallelWeightedLouvain extends Algorithm<ParallelWeightedLouvain> implements LouvainAlgorithm {

    private static final Direction D = Direction.OUTGOING;

    private RelationshipIterator relationshipIterator;
    private RelationshipWeights relationshipWeights;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private IntObjectMap<IntSet> communities; // bag of communityId -> member-nodes
    private final IdMapping idMapping;
    private int iterations;
    private double m2, mq2;
    private final int maxIterations;
    private final AtomicInteger queue;
    private final List<Task> tasks;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    public ParallelWeightedLouvain(IdMapping idMapping,
                                   RelationshipIterator relationshipIterator,
                                   RelationshipWeights relationshipWeights,
                                   ExecutorService executorService,
                                   int concurrency, int maxIterations) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.relationshipIterator = relationshipIterator;
        this.relationshipWeights = relationshipWeights;
        this.executorService = executorService;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        communityIds = new int[nodeCount];
        communities = new IntObjectScatterMap<>();
        queue = new AtomicInteger(0);
        tasks = new ArrayList<>();
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
    public ParallelWeightedLouvain release() {
        relationshipIterator = null;
        relationshipWeights = null;
        executorService = null;
        communities = null;
        return this;
    }

    private void reset() {

        tasks.clear();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }

        communities.clear();

        // TODO find better way for init
        for (int i = 0; i < nodeCount; i++) {
            final IntScatterSet set = new IntScatterSet();
            set.add(i);
            communities.put(i, set);
            communityIds[i] = i;
        }
        final DoubleAdder m = new DoubleAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                m.add(relationshipWeights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
        });
        m2 = m.doubleValue() * 2.0; // 2m
        mq2 = 2.0 * Math.pow(m.doubleValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) {
        writeLock.lock();
        int sourceCommunity = communityIds[node];

        System.out.println("assign " + node + " to " + sourceCommunity + " -> " + targetCommunity);

        // remove node from its old set
        communities.get(sourceCommunity).removeAll(node);
        // place it into its new set
        communities.get(targetCommunity).add(node);
        // update communityIds
        communityIds[node] = targetCommunity;
        writeLock.unlock();
    }

    /**
     * sum of weights of nodes in the community pointing anywhere
     * @return sTot
     */
    private double sTot(int community) {
        double[] stot = {0.0}; // {sTot}
        final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        communities.get(community)
                .forEach((IntProcedure) node -> {
                    relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                        final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                        stot[0] += weight;
                        return true;
                    });
                });
        readLock.unlock();
        return stot[0];
    }

    /**
     * return sum of weights of a given node pointing into given community | v->C
     * @return kiIn
     */
    private double kIIn(int node, int targetCommunity) {
        double[] sum = {0.0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            }
            return true;
        });
        return sum[0];
    }

    private double kI(int node) {
        double[] sum = {0.0}; // {ki}

        final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
        readLock.lock();
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            sum[0] += weight;
            return true;
        });
        readLock.unlock();
        return sum[0];
    }


    private class Task implements Runnable {

        private boolean changes = false;
        private final ProgressLogger logger = getProgressLogger();

        @Override
        public void run() {
            changes = false;
            final double[] bestGain = {0};
            final int[] bestCommunity = {0};
            for (int node; (node = queue.getAndIncrement()) < nodeCount && running(); ) {
                bestGain[0] = 0.0;
                final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
                readLock.lock();
                final int sourceCommunity = bestCommunity[0] = communityIds[node];
                final double mSource = (sTot(sourceCommunity) * kI(node)) / mq2;
                readLock.unlock();
                relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                    readLock.lock();
                    final int targetCommunity = communityIds[targetNodeId];
                    final double gain = kIIn(sourceNodeId, targetCommunity) / m2 - mSource;
                    readLock.unlock();
                    if (gain > bestGain[0]) {
                        bestCommunity[0] = targetCommunity;
                        bestGain[0] = gain;
                    }
                    return true;
                });
                if (bestCommunity[0] != sourceCommunity) {
                    assign(node, bestCommunity[0]);
                    changes = true;
                }
                logger.logProgress(node, nodeCount - 1);
            }
        }
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

    public int getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < communityIds.length; i++) {
            bitSet.put(communityIds[i]);
        }
        return bitSet.size();
    }

    @Override
    public ParallelWeightedLouvain me() {
        return this;
    }

}
