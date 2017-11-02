package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class ParallelLouvain extends Algorithm<ParallelLouvain> {

    private static final Direction D = Direction.BOTH;

    private RelationshipIterator relationshipIterator;
    private RelationshipWeights relationshipWeights;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;
    private final IdMapping idMapping;
    private int iterations;
    private double m2, mq2;
    private final AtomicInteger queue;
    private ArrayList<Task> tasks = new ArrayList<>();
    private final Communities communities;

    public ParallelLouvain(IdMapping idMapping,
                           RelationshipIterator relationshipIterator,
                           RelationshipWeights relationshipWeights,
                           ExecutorService executorService,
                           int concurrency) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.relationshipIterator = relationshipIterator;
        this.relationshipWeights = relationshipWeights;
        this.executorService = executorService;
        this.concurrency = concurrency;
        queue = new AtomicInteger(0);
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        communities = new Communities(nodeCount);
        reset();
    }

    public ParallelLouvain compute(int maxIterations) {
        reset();
        communities.print();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            final boolean compute = compute();
            System.out.println("compute = " + compute);
            if (!compute) {
                System.out.println("iterations = " + iterations);
                return this;
            }
            communities.flip();
            communities.print();
        }
        System.out.println("iterations = " + iterations);
        throw new IllegalStateException("max iterations reached");
//        return this;
    }

    private boolean compute() {
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (Task task : tasks) {
            futures.add(executorService.submit(task));
        }
        ParallelUtil.awaitTermination(futures);
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
        relationshipWeights = null;
        executorService = null;
        return this;
    }

    private void reset() {
        final DoubleAdder adder = new DoubleAdder();

        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            final double[] sw = {0.0};
            relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                sw[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                return true;
            });
            communities.reset(node, sw[0]);
            adder.add(sw[0]);
        });
        m2 = adder.doubleValue() * 2.0; // 2m
        mq2 = 2.0 * Math.pow(adder.doubleValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) {
//        final double nodeWeight = sumWeights[node];
//        sTot.fetchAndAdd(sourceCommunity, -nodeWeight);
//        sTot.fetchAndAdd(targetCommunity, nodeWeight);
//        community.set(node, targetCommunity);
        communities.move(node, targetCommunity);
    }

    /**
     * return sum of weights of a given node pointing into given community | v->C
     * @return kiIn
     */
    private double kIIn(int node, int targetCommunity) {
        double[] sum = {0.0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            if (targetCommunity == communities.get(targetNodeId)) {
                sum[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            }
            return true;
        });

        return sum[0];
    }

    private double kI(int node) {
        double[] sum = {0.0}; // {ki}
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            sum[0] += weight;
            return true;
        });
        return sum[0];
    }



    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(idMapping.toOriginalNodeId(i), communities.get(i), communities.getSTot(i)));
    }


    public int getIterations() {
        return iterations;
    }

    @Override
    public ParallelLouvain me() {
        return this;
    }

    private class Task implements Runnable {

        private boolean changes;
        private double bestGain;
        private int bestCommunity;

        @Override
        public void run() {
            changes = false;
            for (int node; (node = queue.getAndIncrement()) < nodeCount; ) {
                bestGain = 0.0;
                final int sourceCommunity = bestCommunity = communities.get(node);
                final double mSource = (communities.getSTot(node) * kI(node)) / mq2;
                relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                    final int targetCommunity = communities.get(targetNodeId);
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

        public boolean isChanges() {
            return changes;
        }
    }

    public static class Result {

        public final long nodeId;
        public final long community;
        private final double mod;

        public Result(long nodeId, int community, double mod) {
            this.nodeId = nodeId;
            this.community = community;
            this.mod = mod;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", community=" + community +
                    ", mod=" + mod +
                    '}';
        }
    }
}
