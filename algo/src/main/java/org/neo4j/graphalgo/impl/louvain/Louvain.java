package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * sequential weighted undirected modularity based community detection
 * (first phase of louvain algo)
 *
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> implements LouvainAlgorithm {

    public static final Direction D = Direction.OUTGOING;
    public static final int EMPTY = -1;

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

    /**
     * init ki, sTot & m
     */
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

    /**
     * compute first phase louvain
     * @return
     */
    public Louvain compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        improvement = true;
        iterations = 0;
        final NodeIterator rndIter = new ShuffledNodeIterator(graph, nodeCount);
        for (; iterations < maxIterations && improvement; iterations++) {
            improvement = false;
            rndIter.forEachNode(node -> {
                improvement |= compute(node);
                progressLogger.logProgress(node, nodeCount, () -> String.format("Iteration %d", iterations));
                return true;
            });
        }
        progressLogger.logDone();
        return this;
    }

    /**
     * calc mod-gain for a node and move it into its best community
     * @param node node id
     * @return true if the node moved into another community
     */
    private boolean compute(int node) {
        final int currentCommunity = bestCommunity = community[node];
        sTot[currentCommunity] -= ki[node];
        community[node] = EMPTY;
        bestGain = .0;
        forEachConnectedCommunity(node, c -> {
            final double g = 2. * weightIntoCom(node, c) - sTot[c] * ki[node] / m;
            if (g > bestGain) {
                bestGain = g;
                bestCommunity = c;
            }
        });
        sTot[bestCommunity] += ki[node];
        community[node] = bestCommunity;
        return bestCommunity != currentCommunity;
    }

    /**
     * run consumer of each connected community once
     * @param node node id
     * @param consumer community id consumer
     */
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

    /**
     * sum weights from node into community c
     * @param node node id
     * @param c community id
     * @return sum of weights from node into community c
     */
    private double weightIntoCom(int node, int c) {
        final Pointer.DoublePointer p = Pointer.wrap(.0);
        graph.forEachRelationship(node, D, (s, t, r) -> {
            if (community[t] == c) {
                p.v += graph.weightOf(s, t);
            }
            return true;
        });
        return p.v;
    }

    /**
     * get communities
     * @return node-id to community id mapping
     */
    @Override
    public int[] getCommunityIds() {
        return community;
    }

    /**
     * number of iterations until convergence
     * @return number of iterations
     */
    @Override
    public int getIterations() {
        return iterations;
    }

    /**
     * calculate number of communities
     * @return community count
     */
    @Override
    public long getCommunityCount() {
        final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            bitSet.put(community[i]);
        }
        return bitSet.size();
    }

    /**
     * return a stream of nodeId-CommunityId tuples
     * @return result tuple stream
     */
    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), community[i]));
    }

    /**
     * @return this
     */
    @Override
    public Louvain me() {
        return this;
    }

    /**
     * release structures
     * @return this
     */
    @Override
    public Louvain release() {
        graph = null;
        sTot = null;
        ki = null;
        return this;
    }
}
