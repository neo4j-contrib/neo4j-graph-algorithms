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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.concurrent.ExecutorService;

abstract class BaseLabelPropagation<
        G extends Graph,
        W extends WeightMapping,
        L extends LabelPropagationAlgorithm.Labels,
        Self extends BaseLabelPropagation<G, W, L, Self>
        > extends LabelPropagationAlgorithm<Self> {

    static final int[] EMPTY_INTS = new int[0];
    static final long[] EMPTY_LONGS = new long[0];

    private G graph;
    private final long nodeCount;
    private final AllocationTracker tracker;
    private final W nodeProperties;
    private final W nodeWeights;
    final int batchSize;
    final int concurrency;
    final ExecutorService executor;

    private L labels;
    private long ranIterations;
    private boolean didConverge;

    BaseLabelPropagation(
            G graph,
             W nodeProperties,
             W nodeWeights,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.nodeWeights = nodeWeights;
    }

    abstract L initialLabels(long nodeCount, AllocationTracker tracker);


    abstract List<BaseStep> baseSteps(
            final G graph,
            final L labels,
            final W nodeProperties,
            final W nodeWeights,
            final Direction direction,
            final boolean randomizeOrder);

    @Override
    final Self compute(
            Direction direction,
            long maxIterations,
            boolean randomizeOrder) {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        if (labels == null || labels.size() != nodeCount) {
            labels = initialLabels(nodeCount, tracker);
        }

        ranIterations = 0L;
        didConverge = false;

        List<BaseStep> baseSteps = baseSteps(graph, labels, nodeProperties, nodeWeights, direction, randomizeOrder);

        for (long i = 0L; i < maxIterations; i++) {
            ParallelUtil.runWithConcurrency(concurrency, baseSteps, executor);
        }

        long maxIteration = 0L;
        boolean converged = true;
        for (BaseStep baseStep : baseSteps) {
            Step current = baseStep.current;
            if (current instanceof BaseLabelPropagation.Computation) {
                Computation step = (Computation) current;
                if (step.iteration > maxIteration) {
                    maxIteration = step.iteration;
                }
                converged = converged && !step.didChange;
                step.release();
            }
        }

        ranIterations = maxIteration;
        didConverge = converged;

        return me();
    }

    final BaseStep asStep(Initialization initialization) {
        return new BaseStep(initialization);
    }

    @Override
    public final long ranIterations() {
        return ranIterations;
    }

    @Override
    public final boolean didConverge() {
        return didConverge;
    }

    @Override
    public final Labels labels() {
        return labels;
    }

    @Override
    public Self release() {
        graph = null;
        return me();
    }

    static abstract class Initialization implements Step {
        abstract void setExistingLabels();

        abstract Computation computeStep();

        @Override
        public final void run() {
            setExistingLabels();
        }

        @Override
        public final Step next() {
            return computeStep();
        }
    }

    static abstract class Computation implements Step {

        private boolean didChange = true;
        private long iteration = 0L;

        abstract boolean computeAll();

        abstract void release();

        @Override
        public final void run() {
            if (this.didChange) {
                iteration++;
                didChange = computeAll();
                if (!didChange) {
                    release();
                }
            }
        }

        @Override
        public final Step next() {
            return this;
        }
    }

    interface Step extends Runnable {
        @Override
        void run();

        Step next();
    }

    static final class BaseStep implements Runnable {

        private Step current;

        BaseStep(final Step current) {
            this.current = current;
        }

        @Override
        public void run() {
            current.run();
            current = current.next();
        }
    }
}
