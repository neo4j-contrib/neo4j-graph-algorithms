package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
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

    private final Degrees degrees;
    private RelationshipIterator relationshipIterator;
    private RelationshipWeights relationshipWeights;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private IntObjectMap<IntSet> communities; // bag of communityId -> member-nodes
    private final IdMapping idMapping;
    private int iterations;
    private double m, m2, mq2;

    public Louvain(IdMapping idMapping,
                   Degrees degrees,
                   RelationshipIterator relationshipIterator,
                   RelationshipWeights relationshipWeights,
                   ExecutorService executorService,
                   int concurrency) {

        this.idMapping = idMapping;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.degrees = degrees;
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
        throw new IllegalStateException("did not converge");
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
            relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                adder.add(relationshipWeights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
        });
        m = adder.doubleValue();
        m2 = adder.doubleValue() * 2.0; // 2m
        mq2 = 2.0 * Math.pow(adder.doubleValue(), 2.0); // 2m^2
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param targetCommunity communityId
     */
    private void assign(int node, int sourceCommunity, int targetCommunity) {
        // remove node from its old set
        communities.get(sourceCommunity).removeAll(node);
        // place it into its new set
        communities.get(targetCommunity).add(node);
        // update communityIds
        communityIds[node] = targetCommunity;
    }

    private double vToC(int nodeV, int communityC) {
        double[] sum = {0.0};
        relationshipIterator.forEachRelationship(nodeV, D, (sourceNodeId, targetNodeId, relationId) -> {
            final int targetCommunity = communityIds[targetNodeId];
            if (communityC == targetCommunity) {
                sum[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            }
            return true;
        });
        return sum[0];
    }

    private double cToV(int nodeV, int communityC) {
        double[] sum = {0.0};
        communities.get(communityC)
                .forEach((IntProcedure) nodeInC -> {
                    relationshipIterator.forEachRelationship(nodeInC, D, (sourceNodeId, targetNodeId, relationId) -> {
                        sum[0] += relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                        return true;
                    });
                });
        return sum[0];
    }

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
     * return tuple of kI and kI_in
     * kI: sum of weights of all relationships of a given node
     * kI_in: sum of weights of a given node pointing into given community  |  v->C
     * @return {kI, kI_in}
     */
    private double[] kIAndkIIn(int node, int targetCommunity) {
        double[] sum = {0.0, 0.0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
            final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
            sum[0] += weight;
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[1] += weight;
            }
            return true;
        });

        return sum;
    }

    /**
     * return tuple of sIn and sTot for a given community
     * sIn: sum of weights of nodes within the community pointing to node in the same community
     * sTot: sum of weights of nodes within the community pointing anywhere
     * @return {sIn, sTot}
     */
    private double[] sInAndsTot(int community) {
        final IntSet set = communities.get(community); //.getOrDefault(community, EMPTY);
        double[] sum = {0.0, 0.0}; // {sIn, sTot}
        set.forEach((IntProcedure) node -> {
            int nodeCommunity = communityIds[node];
            relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                if (communityIds[targetNodeId] == nodeCommunity) {
                    sum[0] += weight;
                }
                sum[1] += weight;
                return true;
            });
        });
        return sum;
    }

    /**
     * calculate modularity-delta of moving node into a new community
     *
     * @param node
     * @param targetCommunity
     * @return
     */
    private double modGain(int node, int targetCommunity) {
        final double[] sInTot = sInAndsTot(targetCommunity);
        final double[] kIAndkIIn = kIAndkIIn(node, targetCommunity);
//        return (((sInTot[0] + kIAndkIIn[1]) / m2) -                 // (sIn + kIIn / 2*m) -
//                Math.pow((sInTot[1] + kIAndkIIn[0]) / m2, 2)) -     // ((sTot + kI) / 2)^2 -
//                ((sInTot[0] / m2) - Math.pow(sInTot[1] / m2, 2) -   // ((sIn / 2 * m) - (sTot / 2 * m)^2) -
//                        Math.pow(kIAndkIIn[0] / m2, 2));            // (kI / 2 * m)^2
//        return (kIAndkIIn[1] / (2 * m)) - (sTot(targetCommunity) * kIAndkIIn[0] / (2 * Math.pow(m, 2)));
        return (kIAndkIIn[1] / (2 * m)) - (sInTot[1] * kIAndkIIn[1] / (2 * Math.pow(m, 2)));
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
            bestGain[0] = -1;
            final int currentCommunity = communityIds[node];
            bestCommunity[0] = currentCommunity;
            relationshipIterator.forEachRelationship(node, D, (sourceNodeId, targetNodeId, relationId) -> {
                final int targetCommunity = communityIds[targetNodeId];
                final double gain = modGain(sourceNodeId, targetCommunity);
                if (gain > bestGain[0]) {
                    bestCommunity[0] = targetCommunity;
                    bestGain[0] = gain;
                }
                return true;
            });
            if (bestCommunity[0] != currentCommunity && bestGain[0] > 0.0) {
                System.out.println();
                System.out.println("node = " + node);
                System.out.println("currentCommunity = " + currentCommunity + " :" + communities.get(currentCommunity));
                System.out.println("bestCommunity = " + bestCommunity[0] + " :" + communities.get(bestCommunity[0]));
                System.out.println("bestGain = " + bestGain[0]);


                assign(node, currentCommunity, bestCommunity[0]);

                changes[0] = true;
            }
        }
        System.out.println("changes[0] = " + changes[0]);
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
        final IntScatterSet set = new IntScatterSet();
        for (int i = 0; i < communityIds.length; i++) {
            set.add(communityIds[i]);
        }
        return set.size();
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
