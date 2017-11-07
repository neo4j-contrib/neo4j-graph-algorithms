package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDisjointSetStruct;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

public enum UnionFindAlgo {

    QUEUE {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindQueue algo = new ParallelUnionFindQueue(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindQueue)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindQueue algo = new HugeParallelUnionFindQueue(
                    hugeGraph,
                    executor,
                    minBatchSize,
                    concurrency,
                    tracker);
            prepare.accept("CC(HugeParallelUnionFindQueue)", algo);
            HugeDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    FORK_JOIN {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindForkJoin algo = new ParallelUnionFindForkJoin(
                    graph,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindForkJoin)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindForkJoin algo = new HugeParallelUnionFindForkJoin(
                    hugeGraph,
                    tracker,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(HugeParallelUnionFindForkJoin)", algo);
            HugeDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    FJ_MERGE {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindFJMerge algo = new ParallelUnionFindFJMerge(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindFJMerge)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindFJMerge algo = new HugeParallelUnionFindFJMerge(
                    hugeGraph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(HugeParallelUnionFindFJMerge)", algo);
            HugeDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    SEQ {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            GraphUnionFind algo = new GraphUnionFind(graph);
            prepare.accept("CC(SequentialUnionFind)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeGraphUnionFind algo = new HugeGraphUnionFind(
                    hugeGraph,
                    AllocationTracker.EMPTY);
            prepare.accept("CC(HugeSequentialUnionFind)", algo);
            HugeDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    };

    public static BiConsumer<String, Algorithm<?>> NOTHING = (s, a) -> {
    };

    abstract DSSResult run(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

    abstract DSSResult run(
            HugeGraph hugeGraph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

    public final DSSResult runAny(
            Graph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare) {
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return run(
                    hugeGraph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency,
                    threshold,
                    prepare);
        } else {
            return run(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    threshold,
                    prepare);
        }
    }

    public final DSSResult run(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency) {
        return run(
                graph,
                executor,
                minBatchSize,
                concurrency,
                Double.NaN,
                NOTHING);
    }

    public final DSSResult run(
            HugeGraph hugeGraph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        return run(
                hugeGraph,
                executor,
                tracker,
                minBatchSize,
                concurrency,
                Double.NaN,
                NOTHING
        );
    }
}
