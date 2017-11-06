package org.neo4j.graphalgo.impl.louvain;


import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ParallelLouvain extends Algorithm<ParallelLouvain> implements LouvainAlgorithm {

    private RelationshipIterator relationshipIterator;
    private final Degrees degrees;
    private final ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private final AtomicInteger queue;

    private int[] communityIds; // node to community mapping
    private double[] sTot;
    private IdMapping idMapping;
    private ArrayList<Task> tasks = new ArrayList<>();
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private int iterations;
    private double m2, mq2;


    public ParallelLouvain(IdMapping idMapping,
                           RelationshipIterator relationshipIterator,
                           Degrees degrees,
                           ExecutorService executorService,
                           int concurrency) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.relationshipIterator = relationshipIterator;
        this.degrees = degrees;
        this.executorService = executorService;
        this.concurrency = concurrency;
        communityIds = new int[nodeCount];
        sTot = new double[nodeCount];
        this.queue = new AtomicInteger(0);

    }

    public LouvainAlgorithm compute(int maxIterations) {
        reset();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            queue.set(0);
            if (!compute()) {
                return this;
            }
        }
        return this;
    }

    private boolean compute() {
        ParallelUtil.runWithConcurrency(concurrency, tasks, getTerminationFlag(), executorService);

        for (Task task : tasks) {
            if (task.changes) {
                return true;
            }
        }
        return false;
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
            final int d = degrees.degree(node, Direction.BOTH);
            sTot[node] = d;
            adder.add(d);
        });
        m2 = adder.intValue() * 4.0; // 2m //
        mq2 = 2.0 * Math.pow(adder.intValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) { // TODO sync
        final int d = degrees.degree(node, Direction.BOTH);

        final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
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
        relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
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

    public int getCommunityCount() {
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

        @Override
        public void run() {
            changes = false;
            bestGain = 0.0;
            final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

            for (int node; (node = queue.getAndIncrement()) < nodeCount; ) {
                bestGain = 0.0;
                readLock.lock();
                final int sourceCommunity = bestCommunity = communityIds[node];
                final double mSource = (sTot[sourceCommunity] * degrees.degree(node, Direction.BOTH)) / mq2;
                readLock.unlock();

                relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                    readLock.lock();
                    final int targetCommunity = communityIds[targetNodeId];
                    readLock.unlock();
                    final double gain = kIIn(sourceNodeId, targetCommunity) / m2 - mSource;
                    if (gain > bestGain) {
                        bestCommunity = targetCommunity;
                        bestGain = gain;
                    }
                    return true;
                });
                if (bestCommunity != sourceCommunity) {
                    assign(node, bestCommunity);
                    changes = true;
                }
            }
        }
    }

}
