package org.neo4j.prof;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Defaults;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.Collection;
import java.util.Collections;

public final class Heap implements InternalProfiler {

    private long before;

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
        before = usedHeap();
    }

    @Override
    public Collection<? extends Result> afterIteration(
            final BenchmarkParams benchmarkParams,
            final IterationParams iterationParams,
            final IterationResult result) {
        long allUsage = usedHeap() - before;
        long ops = result.getMetadata().getAllOps();
        double perOpUsage = (double) allUsage / ops;

        return Collections.singletonList(
                new ScalarResult(Defaults.PREFIX + "heap.alloc.norm", perOpUsage, "B/op", AggregationPolicy.AVG)
        );
    }

    @Override
    public String getDescription() {
        return "Simple, naive Heap consumption during benchmark. For more detailed profiling, use gc profiler.";
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
