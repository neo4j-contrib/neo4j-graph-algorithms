package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    private int iterations;
    private int bestCommunity;
    private double bestGain;
    private boolean improvement;
    private final SimpleBitSet visited;


    public Louvain(Graph graph, int maxIterations) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxIterations = maxIterations;
        ki = new double[nodeCount];
        community = new int[nodeCount];
        sTot = new double[nodeCount];
        visited = new SimpleBitSet(nodeCount);
        init();
    }

    private void init() {
        for (int node = 0; node < nodeCount; node++) {
            graph.forEachRelationship(node, D, (s, t, r) -> {
                final double w = graph.weightOf(s, t);
                m += w;
                ki[s] += w;
                ki[t] += w;
                return true;
            });
        }
        Arrays.setAll(sTot, i -> ki[i]);
        Arrays.setAll(community, i -> i);
    }

    public Louvain compute() {
        improvement = true;
        iterations = 0;
        final NodeIterator rndIter = new ShuffledNodeIterator(graph, nodeCount);
        for (; iterations < maxIterations && improvement; iterations++) {
            improvement = false;
            rndIter.forEachNode(node -> {
                improvement |= compute(node);
                return true;
            });
        }
        return this;
    }

    private boolean compute(int node) {
        final int nodeCommunity = community[node];
        bestCommunity = nodeCommunity;
        sTot[nodeCommunity] -= ki[node];
        community[node] = -1;
        bestGain = .0;
        forEachConnectedCommunity(node, c -> {
            final double s = weightIntoCom(node, c);
            final double g = 2. * s - sTot[c] * ki[node] / m;
            if (g > bestGain) {
                bestGain = g;
                bestCommunity = c;
            }
        });
        final int best = bestCommunity;
        sTot[best] += ki[node];
        community[node] = best;
        return best != nodeCommunity;
    }

    private void forEachConnectedCommunity(int node, IntConsumer consumer) {
        visited.clear();
        graph.forEachRelationship(node, D, (s, t, r) -> {
            final int c = community[t];
            if (visited.contains(c)) {
                return true;
            }
            visited.put(c);
            consumer.accept(c);
            return true;
        });
    }

    private double weightIntoCom(int node, int c) {
        final double[] shared = {.0};
        graph.forEachRelationship(node, D, (s, t, r) -> {
            if (community[t] == c) {
                shared[0] += graph.weightOf(s, t);
            }
            return true;
        });
        return shared[0];
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
