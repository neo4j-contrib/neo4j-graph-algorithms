package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

/**
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> {

    interface TargetCommunity {
        void apply(int node, int community);
    }

    public static final Direction D = Direction.OUTGOING;
    private final Graph graph;

    private final int nodeCount;

    private double m;
    private final double[] s_tot;
    private final double[] ki;
    private final double[] w;
    private final int[] community;


    public Louvain(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        ki = new double[nodeCount];
        w = new double[nodeCount];
        community = new int[nodeCount];
        s_tot = new double[nodeCount];

        init();
    }

    private void init() {

        Arrays.setAll(community, i -> i);

        forEachRelation((s, t, r) -> {
            final double w = graph.weightOf(s, t);
            m += w;
            ki[s] += w;
            ki[t] += w;
            return true;
        });

        Arrays.setAll(s_tot, i -> ki[i]);
    }

    private void initPartition() {
    }

    public Louvain compute() {

        final int[] bestPartition = new int[nodeCount];
        Arrays.setAll(bestPartition, i -> i);

        final int[] bestCommunity = {-1};
        final double[] bestGain = {.0};
        final double[] bestShared = {.0};
        final boolean[] imp = {false};

        do { // do while improvement
            System.arraycopy(bestPartition, 0, community, 0, nodeCount);
            imp[0] = false;
            graph.forEachNode(node -> {
                final int nodeCommunity = bestPartition[node];
                bestCommunity[0] = nodeCommunity;
                bestPartition[0] = -1;
                bestShared[0] = weightIntoCom(node, community[node]);
                s_tot[nodeCommunity] -= ki[node];
                community[node] = -1;
                forEachNeighborPerCommunityOnce(node, (t, c) -> {
                    final double s = weightIntoCom(node, c);
                    final double g = 2. * s - s_tot[c] * ki[node] / m;
                    if (g > bestGain[0]) {
                        bestGain[0] = g;
                        bestCommunity[0] = c;
                        bestShared[0] = s;
                    }
                });
                final int best = bestCommunity[0];
                s_tot[best] += ki[node];
                bestPartition[node] = best;
                if (best != nodeCommunity) imp[0] = true;

                return true;
            });
        } while (imp[0]);

        return this;
    }

    private double weightIntoCom(int node, int com) {
        final double[] shared = {.0};
        graph.forEachRelationship(node, D, (s, t, r) -> {
            if (community[t] == com) {
                shared[0] += graph.weightOf(s, t);
            }
            return true;
        });
        return shared[0];
    }


    private void forEachNeighborPerCommunityOnce(int node, TargetCommunity consumer) {
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        graph.forEachRelationship(node, D, (s, t, r) -> {
            final int c = community[t];
            if (visited.contains(c)) {
                return true;
            }
            visited.put(c);
            consumer.apply(t, c);
            return true;
        });
    }

    private void forEachRelation(RelationshipConsumer consumer) {
        graph.forEachNode(node -> {
            graph.forEachRelationship(node, D, consumer);
            return true;
        });
    }

    @Override
    public Louvain me() {
        return this;
    }

    @Override
    public Louvain release() {
        return null;
    }
}
