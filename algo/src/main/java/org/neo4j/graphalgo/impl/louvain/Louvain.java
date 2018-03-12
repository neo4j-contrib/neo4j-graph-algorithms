/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.sources.ShuffledNodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * sequential weighted undirected modularity based localCommunities detection
 * (first phase of louvain algo)
 *
 * @author mknblch
 */
public class Louvain extends Algorithm<Louvain> implements LouvainAlgorithm {

    /**
     * only outgoing directions are visited since the graph itself has to
     * be loaded as undirected.
     */
    private static final Direction D = Direction.OUTGOING;
    private static final int NONE = -1;
    private static final double MINIMUM_MODULARITY = Double.NEGATIVE_INFINITY; // -1.0;

    private Graph graph;
    private ExecutorService pool;
    private NodeIterator nodeIterator;
    private final int nodeCount;
    private final int maxIterations;
    private final int concurrency;
    private final AllocationTracker tracker;
    private double m, m2;
    private int[] communities;
    private double[] ki;
    private int iterations;
    private double q = MINIMUM_MODULARITY;
    private AtomicInteger counter = new AtomicInteger(0);

    public Louvain(Graph graph, int maxIterations, ExecutorService pool, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.maxIterations = maxIterations;
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.nodeIterator = new ShuffledNodeIterator(nodeCount);
        ki = new double[nodeCount];
        communities = new int[nodeCount];
        // (1x double + 1x int) * N
        tracker.add(12 * nodeCount);
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
        Arrays.setAll(communities, i -> i);
        progressLogger.logDone(() -> "Init complete");
    }

    /**
     * compute first phase louvain
     * @return
     */
    public Louvain compute() {
        // init helper values & initial community structure
        init();
        final ProgressLogger progressLogger = getProgressLogger();
        // create an array of tasks for parallel exec
        final ArrayList<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        // (2x double + 1x int) * N * threads
        tracker.add(20 * nodeCount * concurrency);
        // as long as maxIterations is not reached
        for (iterations = 0; iterations < maxIterations; iterations++) {
            // reset node counter (for logging)
            counter.set(0);
            // run all tasks
            ParallelUtil.runWithConcurrency(concurrency, tasks, pool);
            // take the best candidate
            Task candidate = best(tasks);
            if (null == candidate || candidate.q <= this.q) {
                // best candidate's modularity did not improve
                break;
            }
            // memorize current modularity
            this.q = candidate.q;
            // sync all tasks with the best candidate for the next round
            sync(candidate, tasks);
            progressLogger.logDone(() -> String.format("Iteration %d led to a modularity %.4f", iterations, q));
        }
        tracker.remove(20 * nodeCount * concurrency);
        progressLogger.logDone(() -> String.format("Done in %d iterations with Q=%.5f)", iterations, q));
        return this;
    }

    /**
     * get the task with the best community distribution
     * (highest modularity value) of an array of tasks
     *
     * @return best task
     */
    private static Task best(Collection<Task> tasks) {
        Task best = null;
        double q = MINIMUM_MODULARITY;
        for (Task task : tasks) {
            final double modularity = task.getModularity();
            if (modularity > q) {
                q = modularity;
                best = task;
            }
        }
        return best;
    }

    /**
     * sync parent Task with all other task except itself and
     * copy community structure to global community structure
     */
    private void sync(Task parent, Collection<Task> tasks) {
        for (Task task : tasks) {
            if (task == parent) {
                continue;
            }
            task.sync(parent);
        }
        System.arraycopy(parent.localCommunities, 0, communities, 0, nodeCount);
    }

    /**
     * get communities
     * @return node-id to localCommunities id mapping
     */
    @Override
    public int[] getCommunityIds() {
        return communities;
    }

    /**
     * number of iterations
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
            bitSet.put(communities[i]);
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
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), communities[i]));
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
        this.graph = null;
        this.pool = null;
        this.communities = null;
        this.ki = null;
        tracker.remove(12 * nodeCount);
        return this;
    }

    /**
     * Restartable task to perform modularity optimization
     */
    private class Task implements Runnable {

        private final double[] sTot, sIn;
        private final int[] localCommunities;
        private double bestGain, bestWeight;
        private int bestCommunity;
        private double q = MINIMUM_MODULARITY;

        /**
         * at creation the task copies the community-structure
         * and initializes its helper arrays
         */
        public Task() {
            sTot = new double[nodeCount];
            sIn = new double[nodeCount];
            localCommunities = new int[nodeCount];
            System.arraycopy(ki, 0, sTot, 0, nodeCount);
            System.arraycopy(communities, 0, localCommunities, 0, nodeCount);
            Arrays.fill(sIn, 0.);
        }

        /**
         * copy community structure and helper arrays from parent
         * task into this task
         */
        public void sync(Task parent) {
            System.arraycopy(parent.localCommunities, 0, localCommunities, 0, nodeCount);
            System.arraycopy(parent.sTot, 0, sTot, 0, nodeCount);
            System.arraycopy(parent.sIn, 0, sIn, 0, nodeCount);
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
            final Pointer.BoolPointer improvement = Pointer.wrap(false);
            final int denominator = nodeCount * concurrency;
            nodeIterator.forEachNode(node -> {
                improvement.v |= move(node);
                progressLogger.logProgress(
                        counter.getAndIncrement(),
                        denominator,
                        () -> String.format("Iteration %d", iterations));
                return true;
            });
            if (improvement.v) {
                this.q = modularity();
            }
        }

        /**
         * get the graph modularity of the calculated community structure
         * @return
         */
        public double getModularity() {
            return q;
        }

        /**
         * calc mod-gain for a node and move it into the best community
         * @param node node id
         * @return true if the node has been moved
         */
        private boolean move(int node) {
            final int currentCommunity = bestCommunity = localCommunities[node];
            sTot[currentCommunity] -= ki[node];
            sIn[currentCommunity] -= 2. * weightIntoCom(node, currentCommunity);
            localCommunities[node] = NONE;
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
            localCommunities[node] = bestCommunity;
            return bestCommunity != currentCommunity;
        }

        /**
         * apply consumer to each connected community one time
         * @param node node id
         * @param consumer community id consumer
         */
        private void forEachConnectedCommunity(int node, IntConsumer consumer) {
            final SimpleBitSet visited = new SimpleBitSet(nodeCount);
            graph.forEachRelationship(node, D, (s, t, r) -> {
                final int c = localCommunities[t];
                if (visited.contains(c)) {
                    return true;
                }
                visited.put(c);
                consumer.accept(c);
                return true;
            });
        }

        /**
         * calc graph modularity
         */
        private double modularity() {
            double q = .0;
            final SimpleBitSet bitSet = new SimpleBitSet(nodeCount);
            for (int k = 0; k < nodeCount; k++) {
                final int c = localCommunities[k];
                if (!bitSet.contains(c)) {
                    bitSet.put(c);
                    q += (sIn[c] / m2) - (Math.pow(sTot[c] / m2, 2.));
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
                if (localCommunities[t] == c) {
                    p.v += graph.weightOf(s, t);
                }
                return true;
            });
            return p.v;
        }

    }
}
