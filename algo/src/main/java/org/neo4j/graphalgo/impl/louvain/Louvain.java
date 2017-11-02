package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    private static final Direction D = Direction.BOTH;

    private RelationshipIterator relationshipIterator;
    private final Degrees degrees;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private final double[] sTot;
    private final IdMapping idMapping;
    private int iterations;
    private double m2, mq2;

    public Louvain(IdMapping idMapping,
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

    }

    public Louvain compute(int maxIterations) {
        reset();
        for (this.iterations = 0; this.iterations < maxIterations; this.iterations++) {
            if (!arrange()) {
                return this;
            }
        }
        return this;
    }


    @Override
    public Louvain release() {
        relationshipIterator = null;
        executorService = null;
        return this;
    }

    private void reset() {

        Arrays.setAll(communityIds, i -> i);

        final LongAdder adder = new LongAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            final int d = degrees.degree(node, D);
            sTot[node] = d;
            adder.add(d);
        });
        m2 = adder.doubleValue() * 2.0; // 2m
        mq2 = 2.0 * Math.pow(adder.intValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int targetCommunity) {
        int sourceCommunity = communityIds[node];

        final int d = degrees.degree(node, D);
        sTot[sourceCommunity] -= d;
        sTot[targetCommunity] += d;

        // update communityIds
        communityIds[node] = targetCommunity;
    }

     /**
     * return sum of weights of a given node pointing into given community | v->C
     * @return kiIn
     */
    private int kIIn(int node, int targetCommunity) {
        int[] sum = {0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[0]++;
            }
            return true;
        });

        return sum[0];
    }

    /**
     * implements phase 1 of louvain
     * @return
     */
    private boolean arrange() {
        final boolean[] changes = {false};
        final double[] bestGain = {0};
        final int[] bestCommunity = {0};
        for (int node = 0; node < nodeCount; node++) {
            bestGain[0] = 0.0;
            final int sourceCommunity = bestCommunity[0] = communityIds[node];
            final double mSource = (sTot[sourceCommunity] * degrees.degree(node, D)) / mq2;
            relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                final int targetCommunity = communityIds[targetNodeId];
                final double gain = kIIn(sourceNodeId, targetCommunity) / m2 - mSource;
                if (gain > bestGain[0]) {
                    bestCommunity[0] = targetCommunity;
                    bestGain[0] = gain;
                }
                return true;
            });
            if (bestCommunity[0] != sourceCommunity) {
                assign(node, bestCommunity[0]);
                changes[0] = true;
            }
        }
        return changes[0];
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
    public Louvain me() {
        return this;
    }

    public static class Result {

        public final long nodeId;
        public final long community;

        public Result(long nodeId, int community) {
            this.nodeId = nodeId;
            this.community = community;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", community=" + community +
                    '}';
        }
    }
}
