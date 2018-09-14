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
package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIntersect;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedLongLongBitSet;
import org.neo4j.graphalgo.impl.Algorithm;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiPredicate;

/**
 *
 * @author mknblch
 */
public class HugeLocalTriadDynamics extends Algorithm<HugeLocalTriadDynamics> {

    private HugeGraph graph;
    private ExecutorService executorService;
    private final int concurrency;
    private final long nodeCount;
    private final AllocationTracker tracker;
    private final AtomicLong queue;
    private final AtomicLong visitedNodes;
    private final LongAdder rebalancedTriads = new LongAdder();
    private final PagedLongLongBitSet balance;

    public HugeLocalTriadDynamics(HugeGraph graph, ExecutorService executorService, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
        this.tracker = tracker;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        visitedNodes = new AtomicLong();
        queue = new AtomicLong();
        balance = PagedLongLongBitSet.newBitSet(nodeCount, tracker);
    }

    @Override
    public final HugeLocalTriadDynamics me() {
        return this;
    }

    @Override
    public HugeLocalTriadDynamics release() {
        executorService = null;
        graph = null;
        return this;
    }

    public PagedLongLongBitSet getBalance() {
        return balance;
    }

    public double getWeight(long nodeA, long nodeB) {

        final long mappedA = graph.toMappedNodeId(nodeA);
        final long mappedB = graph.toMappedNodeId(nodeB);
        final double weight = graph.weightOf(mappedA, mappedB);
        if (!balance.getOrDefault(mappedA, mappedB, true)) {
            return -weight;
        }
        return weight;
    }

    public HugeLocalTriadDynamics compute() {
        return compute(0.5);
    }

    public HugeLocalTriadDynamics compute(double probability) {
        visitedNodes.set(0);
        queue.set(0);
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, () -> new HugeTask(graph, probability));
        // run
        ParallelUtil.run(tasks, executorService);
        return this;
    }

    /**
     * get sign of the weight on the relationship between A and B
     * multiplied by the value in the sign-array
     * @param nodeA
     * @param nodeB
     * @return true if positive(friendly), false(unfriendly) otherwise
     */
    public boolean sign(long nodeA, long nodeB) {
        return (graph.weightOf(nodeA, nodeB) >= 0) == balance.getOrDefault(nodeA, nodeB, true); // balanced is default // TODO
    }

    public boolean friendly(long nodeA, long nodeB) {
        return sign(nodeA, nodeB);
    }


    public boolean unfriendly(long nodeA, long nodeB) {
        return !sign(nodeA, nodeB);
    }

    private void flip(long nodeA, long nodeB) {
        balance.flip(nodeA, nodeB);
    }

    /**
     * triangle type
     * @param nodeA
     * @param nodeB
     * @param nodeC
     * @return type (the number of unfriendly links in a triangle)
     */
    public int type(long nodeA, long nodeB, long nodeC) {
        return (sign(nodeA, nodeB) ? 0 : 1) +
                (sign(nodeA, nodeC) ? 0 : 1) +
                (sign(nodeB, nodeC) ? 0 : 1);
    }

    /**
     * check if a triad id balanced
     * @return true if balanced, false otherwise
     */
    public boolean isBalanced(long nodeA, long nodeB, long nodeC) {
        return type(nodeA, nodeB, nodeC) % 2 == 0;
    }

    /**
     * random boolean
     * @param probability
     * @return
     */
    private boolean coinFlip(double probability) {
        return Math.random() > probability;
    }

    /**
     * evaluate type of triangle A-B-C and rebalance with probability p
     * if it is an unbalanced triad (if Type 1 or 3), does nothing otherwise
     * @return true if a sign has been changed due to rebalancing
     */
    private boolean balance(long a, long b, long c, double probability) {

        switch (type(a, b, c)) {
            case 1:
                // Type1: flip a friendly rel with probability p or flip an unfriendly rel. with 1 - p
                if (coinFlip(probability)) {
                    flipFriendly(a, b, c);
                } else {
                    flipUnfriendly(a, b, c);
                }
                return true;
            case 3:
                // Type3: flip an unfriendly relation
                flipUnfriendly(a, b, c);
                return true;
        }

        return false;
    }

    /**
     * flip the sign of relationship A-B if predicate returns true
     * @return true if flipped, false otherwise
     */
    private boolean flipIf(long a, long b, BiPredicate<Long, Long> predicate) {
        if (predicate.test(a, b)) {
            flip(a, b);
            return true;
        }
        return false;
    }

    /**
     * Flip sign of a friendly relationship in triad A-B-C if any
     */
    private void flipFriendly(long a, long b, long c) {
        if (!flipIf(a, b, this::friendly)) {
            if (!flipIf(a, c, this::friendly)) {
                flipIf(b, c, this::friendly);
            }
        }
    }

    /**
     * Flip sign of an unfriendly relationship in triad A-B-C if any
     */
    private void flipUnfriendly(long a, long b, long c) {
        if (!flipIf(a, b, this::unfriendly)) {
            if (!flipIf(a, c, this::unfriendly)) {
                flipIf(b, c, this::unfriendly);
            }
        }
    }

    public static class Result {

        public final long nodeId;

        public Result(long nodeId) {
            this.nodeId = nodeId;
        }
    }

    private class HugeTask implements Runnable, IntersectionConsumer {

        private HugeRelationshipIntersect hg;
        private final double probability;

        HugeTask(HugeGraph graph, double probability) {
            hg = graph.intersectionCopy();
            this.probability = probability;
        }

        @Override
        public void run() {
            long node;
            while ((node = queue.getAndIncrement()) < nodeCount && running()) {
                hg.intersectAll(node, this);
                getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
        }

        @Override
        public void accept(final long a, final long b, final long c) {
            if (isBalanced(a, b, c)) {
                return;
            }
            if (balance(a, b, c, probability)) {
                rebalancedTriads.increment();
            }
        }
    }
}
