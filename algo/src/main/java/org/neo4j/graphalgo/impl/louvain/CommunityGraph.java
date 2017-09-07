package org.neo4j.graphalgo.impl.louvain;


import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * @author mknblch
 */
public class CommunityGraph {

    public static final IntScatterSet EMPTY = new IntScatterSet();

    private final Graph graph;
    private final int nodeCount;

    private final int[] communityIds; // node to community mapping
    private final IntObjectMap<IntSet> communities; // bag of communityId -> member-nodes
    private double m2 = 0.0;

    public CommunityGraph(Graph graph) {
        nodeCount = graph.nodeCount();
        this.graph = graph;
        communityIds = new int[nodeCount];
        communities = new IntObjectScatterMap<>();
    }

    public void reset() {
        Arrays.parallelSetAll(communityIds, i -> i);
        m2 = 0.0;
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                m2 += weight;
                return true;
            });
            return true;
        });
        m2 *= 2.0;
        communities.clear();
    }

    /**
     * assign node to community
     * @param node nodeId
     * @param community communityId
     */
    public void assign(int node, int community) {
        // find current community of node
        final int actualCommunity = communityIds[node];
        if (community == actualCommunity) {
            return;
        }
        // remove node from its old set
        final IntSet sourceSet = communities.get(actualCommunity);
        if (null != sourceSet) {
            sourceSet.removeAll(node);
        }
        // place it into its new set
        IntSet targetSet = communities.get(community);
        if (null == targetSet) {
            targetSet = new IntScatterSet();
        }
        targetSet.add(node);
        // update communityIds
        communityIds[node] = community;
    }

    /**
     * return tuple of sIn and sTot for a given community
     * sIn: sum of weights of nodes within the community pointing to node in the same community
     * sTot: sum of weights of nodes within the community pointing anywhere
     * @return {sIn, sTot}
     */
    public double[] sInAndsTot(int community) {
        final IntSet set = communities.getOrDefault(community, EMPTY);
        double[] sum = {0.0, 0.0}; // {sIn, sTot}
        set.forEach((IntProcedure) node -> {
            graph.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
                // BOTH counts relationships twice therefore count only if id(s) < id(t)
                if (set.contains(targetNodeId) && sourceNodeId < targetNodeId) {
                    sum[0] += weight;
                }
                sum[1] += weight;
                return true;
            });
        });
        return sum;
    }

    /**
     * return tuple of kI and kI_in
     * kI: sum of weights of all relationships of a given node
     * kI_in: sum of weights of a given node pointing into given community
     * @return {kI, kI_in}
     */
    public double[] kIAndkIIn(int node, int targetCommunity) {
        double[] sum = {0.0, 0.0}; // {ki, ki_in}
        graph.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
            sum[0] += weight;
            if (targetCommunity == communityIds[targetNodeId]) {
                sum[1] += weight;
            }
            return true;
        });
        return sum;
    }

    /**
     * calculate modularity-delta of moving node into community
     * @param node
     * @param targetCommunity
     * @return
     */
    public double modGain(int node, int targetCommunity) {
        final double[] sInTot = sInAndsTot(targetCommunity);
        final double[] kIAndkIIn = kIAndkIIn(node, targetCommunity);
        return (((sInTot[0] + kIAndkIIn[1]) / m2) -                 // (sIn + kIIn / 2*m)
                Math.pow((sInTot[1] + kIAndkIIn[0]) / m2, 2)) -     // ((sTot + kI) / 2)^2
                ((sInTot[0] / m2) - Math.pow(sInTot[1] / m2, 2) -   // (sIn / 2 * m) - (sTot / 2 * m)^2
                        Math.pow(kIAndkIIn[0] / m2, 2));            // (kI / 2 * m)^2
    }

    public boolean compute() {
        final boolean[] changes = {false};
        final double[] bestGain = {0};
        final int[] bestCommunity = {0};
        for (int node = 0; node < nodeCount; node++) {
            bestGain[0] = Double.MIN_VALUE;
            final int currentCommunity = communityIds[node];
            bestCommunity[0] = currentCommunity;
            graph.forEachRelationship(node, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
                final int targetCommunity = communityIds[targetNodeId];
                final double gain = modGain(sourceNodeId, targetCommunity);
                if (gain > bestGain[0]) {
                    bestCommunity[0] = targetCommunity;
                    bestGain[0] = gain;
                }
                return true;
            });
            if (bestCommunity[0] > 0.0 && bestCommunity[0] != currentCommunity) {
                assign(node, bestCommunity[0]);
                changes[0] = true;
            }
        }
        return changes[0];
    }

    public int[] getCommunityIds() {
        return communityIds;
    }
}
