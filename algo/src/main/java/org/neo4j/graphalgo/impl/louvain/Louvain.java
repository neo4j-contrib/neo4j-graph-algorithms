package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    private static final Direction D = Direction.BOTH;

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

    public Louvain(IdMapping idMapping,
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
        communityIds = new int[nodeCount];
        communities = new IntObjectScatterMap<>();
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
        relationshipWeights = null;
        executorService = null;
        communities = null;
        return this;
    }

    private void reset() {
        communities.clear();

        // TODO find better way for init
        for (int i = 0; i < nodeCount; i++) {
            final IntScatterSet set = new IntScatterSet();
            set.add(i);
            communities.put(i, set);
            communityIds[i] = i;
        }
        final DoubleAdder adder = new DoubleAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, concurrency, node -> {
            relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                adder.add(relationshipWeights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
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
        int sourceCommunity = communityIds[node];
        // remove node from its old set
        communities.get(sourceCommunity).removeAll(node);
        // place it into its new set
        communities.get(targetCommunity).add(node);
        // update communityIds
        communityIds[node] = targetCommunity;
    }

    /**
     * sum of weights of nodes in the community pointing anywhere
     * @return sTot
     */
    private double sTot(int community) {
        double[] stot = {0.0}; // {sTot}
        communities.get(community)
                .forEach((IntProcedure) node -> {
                    relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                        final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                        stot[0] += weight;
                        return true;
                    });
                });
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
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            sum[0] += weight;
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
            final double mSource = (sTot(sourceCommunity) * kI(node)) / mq2;
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

    public IntObjectMap<IntSet> getCommunities() {
        return communities;
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
