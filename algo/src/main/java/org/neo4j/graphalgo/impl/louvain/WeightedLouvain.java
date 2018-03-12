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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
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
public class WeightedLouvain extends Algorithm<WeightedLouvain> implements LouvainAlgorithm {

    private Graph graph;
    private ExecutorService pool;
    private final int concurrency;
    private final int nodeCount;
    private final int maxIterations;
    private final AtomicInteger queue;
    private final List<Task> tasks;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    // m = sum of all weights *2
    private double m2;
    private double mq2;
    // weighted degree
    private double[] w;
    // node to community
    private volatile int[] nodeCommunity;
    // community weight
    private volatile double[] sTot;
    // number of iterations
    private int iterations;


    public WeightedLouvain(Graph graph, ExecutorService pool, int concurrency, int maxIterations) {
        this.graph = graph;
        this.pool = pool;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxIterations = maxIterations;
        nodeCommunity = new int[nodeCount];
        sTot = new double[nodeCount];
        w = new double[nodeCount];
        queue = new AtomicInteger();
        tasks = new ArrayList<>();
    }

    private void init() {

        tasks.clear();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }

        Arrays.setAll(nodeCommunity, i -> i);
        final DoubleAdder adder = new DoubleAdder();
        ParallelUtil.iterateParallel(pool, nodeCount, concurrency, node -> {
            final double[] ws = {0.0};
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                ws[0] += graph.weightOf(sourceNodeId, targetNodeId);
                return true;
            });
            adder.add(ws[0]);
            this.w[node] = this.sTot[node] = ws[0];
        });

        this.iterations = 0;
        this.m2 = adder.doubleValue(); // 2m
        this.mq2 = Math.pow(adder.doubleValue(), 2.0); // 2m^2
    }

    private void move(int node, int targetCommunity) {
        writeLock.lock();
        final int sourceCommunity = nodeCommunity[node];
        sTot[sourceCommunity] -= w[node];
        sTot[targetCommunity] += w[node];
        nodeCommunity[node] = targetCommunity;
        writeLock.unlock();
    }

    private double weightIntoC(int node, int targetCommunity) {
        final double[] w = {0.0};
        graph.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
            if(nodeCommunity[t] != targetCommunity) return true;
            w[0] += graph.weightOf(s, t);
            return true;
        });
        return w[0];
    }

    private double modGain(int node, int targetCommunity) {
        return (weightIntoC(node, targetCommunity) / m2) - (w[node] * sTot[targetCommunity] / mq2);
    }

    private int bestCommunity(int node) {
        final double[] bestGain = {0.0};
        final int[] bestCommunity = {nodeCommunity[node]};
        graph.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
            final int targetCommunity = nodeCommunity[t];
            final double gain = modGain(node, targetCommunity);
            if (gain >= bestGain[0]) {
                bestGain[0] = gain;
                bestCommunity[0] = targetCommunity;
            }
            return true;
        });
        return bestCommunity[0];
    }

    @Override
    public WeightedLouvain me() {
        return this;
    }

    @Override
    public WeightedLouvain release() {
        graph = null;
        pool = null;
        sTot = null;
        w = null;
        return this;
    }

    @Override
    public LouvainAlgorithm compute() {
        init();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            queue.set(0);
            ParallelUtil.runWithConcurrency(concurrency, tasks, getTerminationFlag(), pool);
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
    public int[] getCommunityIds() {
        return nodeCommunity;
    }

    @Override
    public int getIterations() {
        return iterations;
    }

    @Override
    public long getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            bitSet.put(nodeCommunity[i]);
        }
        return bitSet.size();

    }

    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(graph.toOriginalNodeId(i), nodeCommunity[i]));
    }

    private class Task implements Runnable {

        private boolean changes;

        @Override
        public void run() {
            final ProgressLogger logger = getProgressLogger();
            changes = false;
            for (int node; (node = queue.getAndIncrement()) < nodeCount && running(); ) {
                final int bestCommunity = bestCommunity(node);
                if (bestCommunity != nodeCommunity[node]) {
                    move(node, bestCommunity);
                    changes = true;
                }
                logger.logProgress(node, nodeCount - 1, () -> "Round " + iterations);
            }
        }
    }
}
