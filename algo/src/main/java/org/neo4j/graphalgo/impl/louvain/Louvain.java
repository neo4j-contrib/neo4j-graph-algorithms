package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    public static final IntScatterSet EMPTY = new IntScatterSet();

    private RelationshipIterator relationshipIterator;
    private RelationshipWeights relationshipWeights;
    private ExecutorService executorService;
    private final int concurrency;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private IntObjectMap<IntSet> communities; // bag of communityId -> member-nodes
    private final IdMapping idMapping;
    private double m2;
    private int iterations;


//    public void assign(int node, int community) {
//        // find current community of node
//        final int actualCommunity = communityIds[node];
//        if (community == actualCommunity) {
//            return;
//        }
//        // remove node from its old set
//        final IntSet sourceSet = communities.get(actualCommunity);
//        if (null != sourceSet) {
//            sourceSet.removeAll(node);
//        }
//        // place it into its new set
//        IntSet targetSet = communities.get(community);
//        if (null == targetSet) {
//            targetSet = new IntScatterSet();
//        }
//        targetSet.add(node);
//        // update communityIds
//        communityIds[node] = community;
//    }


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

    public Louvain compute(int iterations) {
        reset();
        for (int i = 0; i < iterations; i++) {
            if (!arrange()) {
                return this; // convergence
            }
        }
        return this;
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i ->
                        new Result(idMapping.toOriginalNodeId(i), communityIds[i]));
    }

    public double communityModularity() {
        double[] mod = {0.0};
        communities.keys().forEach((IntProcedure) c -> {
            final double[] sInAndsTot = sInAndsTot(c);
            mod[0] += (sInAndsTot[0] / m2) - Math.pow((sInAndsTot[1] / m2), 2);
        });
        return mod[0];
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
        IntIntMap map = new IntIntScatterMap();
        // TODO
        for (int i = 0; i < communityIds.length; i++) {
            map.addTo(communityIds[i], 1);
        }
        return map.size();
    }

    @Override
    public Louvain me() {
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
        iterations = 1;
        communities.clear();

        // TODO find better way for init
        for (int i = 0; i < nodeCount; i++) {
            final IntScatterSet set = new IntScatterSet();
            set.add(i);
            communities.put(i, set);
            communityIds[i] = i;
        }
        final DoubleAdder adder = new DoubleAdder();
        ParallelUtil.iterateParallel(executorService, nodeCount, 2 /* TODO */, node -> {
            relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                adder.add(relationshipWeights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
        });
        m2 = adder.doubleValue() * 2d;
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
            relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                final double weight = relationshipWeights.weightOf(sourceNodeId, targetNodeId);
                // BOTH counts relationships twice therefore count only if id(s) < id(t)
                if (sourceNodeId < targetNodeId && communityIds[targetNodeId] == nodeCommunity) {
                    sum[0] += weight;
                }
                // TODO eval
//                if (communityIds[targetNodeId] == nodeCommunity) {
//                    sum[0] += weight;
//                }
                sum[1] += weight;
                return true;
            });
        });
//        System.out.printf("c%d : (%2.3f, %2.3f)%n", community, sum[0], sum[1]);
        return sum;
    }

    /**
     * return tuple of kI and kI_in
     * kI: sum of weights of all relationships of a given node
     * kI_in: sum of weights of a given node pointing into given community
     * @return {kI, kI_in}
     */
    private double[] kIAndkIIn(int node, int targetCommunity) {
        double[] sum = {0.0, 0.0}; // {ki, ki_in}
        relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
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
     * calculate modularity-delta of moving node into a new community
     *
     * @param node
     * @param targetCommunity
     * @return
     */
    private double modGain(int node, int targetCommunity) {
        final double[] sInTot = sInAndsTot(targetCommunity);
        final double[] kIAndkIIn = kIAndkIIn(node, targetCommunity);
        return (((sInTot[0] + kIAndkIIn[1]) / m2) -                 // (sIn + kIIn / 2*m)
                Math.pow((sInTot[1] + kIAndkIIn[0]) / m2, 2)) -     // ((sTot + kI) / 2)^2
                ((sInTot[0] / m2) - Math.pow(sInTot[1] / m2, 2) -   // (sIn / 2 * m) - (sTot / 2 * m)^2
                        Math.pow(kIAndkIIn[0] / m2, 2));            // (kI / 2 * m)^2
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
            bestGain[0] = Double.MIN_VALUE;
            final int currentCommunity = communityIds[node];
            bestCommunity[0] = currentCommunity;
            relationshipIterator.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId) -> {
                final int targetCommunity = communityIds[targetNodeId];
                final double gain = modGain(sourceNodeId, targetCommunity);
//                System.out.printf("%d: c%d -> c%d : %2.3f (%2.3f)%n", sourceNodeId, currentCommunity, targetCommunity, gain, communityModularity());
                if (gain > bestGain[0]) {
                    bestCommunity[0] = targetCommunity;
                    bestGain[0] = gain;
                }
                return true;
            });
            if (bestCommunity[0] != currentCommunity && bestCommunity[0] > 0.0) {
                assign(node, currentCommunity, bestCommunity[0]);

                changes[0] = true;
            }
        }
        return changes[0];
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
