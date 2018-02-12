package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.Pointer.*;

/**
 * sequential weighted undirected louvain
 *
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> implements LouvainAlgorithm {

    public static final Direction D = Direction.OUTGOING;

    private Graph graph;
    private final int nodeCount;
    private double m;
    private double[] sTot;
    private double[] ki;
    private final int[] community;
    private final int maxIterations;
    private int iterations = 0;

    public Louvain(Graph graph, int maxIterations) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxIterations = maxIterations;
        ki = new double[nodeCount];
        community = new int[nodeCount];
        sTot = new double[nodeCount];

        init();
    }

    private void init() {
        forEachRelation((s, t, r) -> {
            final double w = graph.weightOf(s, t);
            m += w;
            ki[s] += w;
            ki[t] += w;
            return true;
        });
        Arrays.setAll(sTot, i -> ki[i]);
    }

    public Louvain compute() {
        final int[] lCommunity = new int[nodeCount];
        Arrays.setAll(lCommunity, i -> i);
        final IntPointer bestCommunity = Pointer.wrap(-1);
        final DoublePointer bestGain = Pointer.wrap(.0);
        final BoolPointer imp = Pointer.wrap(true);
        final NodeIterator rndIter = new ShuffledNodeIterator(graph, nodeCount);
        for (; iterations < maxIterations && imp.v; iterations++) {
            System.arraycopy(lCommunity, 0, this.community, 0, nodeCount);
            imp.v = false;
            rndIter.forEachNode(node -> {
                final int nodeCommunity = lCommunity[node];
                bestCommunity.v = nodeCommunity;
                sTot[nodeCommunity] -= ki[node];
                lCommunity[node] = -1;
                bestGain.v = .0;
                forEachConnectedCommunity(lCommunity, node, c -> {
                    final double s = weightIntoCom(lCommunity, node, c);
                    final double g = 2. * s - sTot[c] * ki[node] / m;
                    if (g > bestGain.v) {
                        bestGain.v = g;
                        bestCommunity.v = c;
                    }
                });
                final int best = bestCommunity.v;
                sTot[best] += ki[node];
                lCommunity[node] = best;
                imp.v |= best != nodeCommunity;
                return true;
            });
        }
        return this;
    }

    @Override
    public int[] getCommunityIds() {
        return community;
    }

    @Override
    public int getIterations() {
        return iterations;
    }

    @Override
    public long getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            bitSet.put(community[i]);
        }
        return bitSet.size();
    }

    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), community[i]));
    }

    private double weightIntoCom(int[] communities, int node, int c) {
        final double[] shared = {.0};
        graph.forEachRelationship(node, D, (s, t, r) -> {
            if (communities[t] == c) {
                shared[0] += graph.weightOf(s, t);
            }
            return true;
        });
        return shared[0];
    }

    private void forEachConnectedCommunity(int[] communities, int node, IntConsumer consumer) {
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        graph.forEachRelationship(node, D, (s, t, r) -> {
            final int c = communities[t];
            if (visited.contains(c)) {
                return true;
            }
            visited.put(c);
            consumer.accept(c);
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
        graph = null;
        sTot = null;
        ki = null;
        return this;
    }
}
