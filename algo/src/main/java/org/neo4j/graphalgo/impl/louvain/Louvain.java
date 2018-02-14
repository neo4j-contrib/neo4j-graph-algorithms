package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
    public static final int NONE = -1;

    private Graph graph;
    private final int nodeCount;
    private double m, m2;
    private final int maxIterations;
    private final ExecutorService pool;
    private final int concurrency;
    private double[] ki;
    private int iterations;
    private int[] community;

    public Louvain(Graph graph, int maxIterations, ExecutorService pool, int concurrency) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxIterations = maxIterations;
        this.pool = pool;
        this.concurrency = concurrency;
        ki = new double[nodeCount];
        init();
    }

    /**
     * init ki, sTot & m
     */
    private void init() {
        final ProgressLogger progressLogger = getProgressLogger();
        for (int node = 0; node < nodeCount; node++) {
            graph.forEachRelationship(node, D, (s, t, r) -> {
                final double w = graph.weightOf(s, t);
                m += w;
                ki[s] += w;
                ki[t] += w;
                return true;
            });
            progressLogger.logProgress(node, nodeCount, () -> "Init");
        }
        m2 = 2 * m;
        progressLogger.logDone(() -> "Init");
    }

    /**
     * compute first phase louvain
     * @return
     */
    public Louvain compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        final ArrayList<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        final Pointer.DoublePointer q = Pointer.wrap(Double.NEGATIVE_INFINITY);
        for (iterations = 0; iterations < maxIterations; iterations++) {
            ParallelUtil.runWithConcurrency(concurrency, tasks, pool);
            Task best = best(tasks);
            if (null == best || best.getModularity() <= 0) {
                break;
            }
            sync(best, tasks);
        }
        this.community = best(tasks).community;
        progressLogger.logDone(() -> "modularity: " + q.v);
        return this;
    }

    private static Task best(Collection<Task> tasks) {
        Task best = null;
        double q = Double.NEGATIVE_INFINITY;
        for (Task task : tasks) {
            final double modularity = task.getModularity();
            if (modularity > q) {
                q = modularity;
                best = task;
            }
        }
        return best;
    }

    private void sync(Task parent, Collection<Task> tasks) {
        for (Task task : tasks) {
            System.arraycopy(parent.community, 0, task.community, 0, nodeCount);
            System.arraycopy(parent.sTot, 0, task.sTot, 0, nodeCount);
            System.arraycopy(parent.sIn, 0, task.sIn, 0, nodeCount);
        }
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
        return this;
    }

    private class Task implements Runnable {

        private final double[] sTot, sIn;
        private final int[] community;

        private double bestGain, bestWeight;
        private int bestCommunity;

        private double q = Double.NEGATIVE_INFINITY;

        public Task() {
            sTot = new double[nodeCount];
            sIn = new double[nodeCount];
            community = new int[nodeCount];
            System.arraycopy(ki, 0, sTot, 0, nodeCount);
            Arrays.setAll(community, i -> i);
            Arrays.fill(sIn, 0.);
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
            final NodeIterator rndIter = new ShuffledNodeIterator(graph, nodeCount);
            final Pointer.IntPointer counter = Pointer.wrap(0);
            final Pointer.BoolPointer improvement = Pointer.wrap(false);
            rndIter.forEachNode(node -> {
                improvement.v |= move(node);
                progressLogger.logProgress(counter.v++, nodeCount, () -> String.format("Iteration %d", iterations));
                return true;
            });
            this.q = improvement.v ? modularity() : Double.NEGATIVE_INFINITY;
        }

        public double getModularity() {
            return q;
        }

        /**
         * calc mod-gain for a node and move it into the best community
         * @param node node id
         * @return true if the node moved into another community
         */
        private boolean move(int node) {
            final int currentCommunity = bestCommunity = community[node];
            sTot[currentCommunity] -= ki[node];
            sIn[currentCommunity] -= 2. * weightIntoCom(node, currentCommunity);
            community[node] = NONE;
            bestGain = .0;
            bestWeight = .0;
            forEachConnectedCommunity(node, c -> {
                final double wic = weightIntoCom(node, c);
                final double g = 2. * wic - sTot[c] * ki[node] / m;
                if (g > bestGain) {
                    bestGain = g;
                    bestCommunity = c;
                    bestWeight = wic;
                }
            });
            sTot[bestCommunity] += ki[node];
            sIn[bestCommunity] += 2. * bestWeight;
            community[node] = bestCommunity;
            return bestCommunity != currentCommunity;
        }

        /**
         * run consumer of each connected community once
         * @param node node id
         * @param consumer community id consumer
         */
        private void forEachConnectedCommunity(int node, IntConsumer consumer) {
            final SimpleBitSet visited = new SimpleBitSet(nodeCount);
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
         * calc modularity
         */
        private double modularity() {
            double q = .0;
            final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
            for (int k = 0; k < nodeCount; k++) {
                final int c = community[k];
                if (!bitSet.contains(c)) {
                    bitSet.put(c);
                    q += (sIn[k] / m2) - (Math.pow(sTot[k] / m2, 2.0));
                }
            }
            return q;
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

    }
}
