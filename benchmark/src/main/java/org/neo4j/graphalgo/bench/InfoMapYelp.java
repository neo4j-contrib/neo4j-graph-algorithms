package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-ea", "-Xms4g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.MINUTES)
public class InfoMapYelp {

    @Benchmark
    public InfoMap infoMap(final InfoMapGraph infoMapGraph) {
        InfoMap infoMap = InfoMap.unweighted(
                infoMapGraph.graph,
                infoMapGraph.pageRanks,
                infoMapGraph.threshold,
                infoMapGraph.tau,
                Pools.FJ_POOL,
                infoMapGraph.concurrency,
                infoMapGraph.progressLogger,
                TerminationFlag.RUNNING_TRUE
        );
        return infoMap.compute();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InfoMapYelp.class.getSimpleName())
                .addProfiler("gc")
                .result(InfoMapYelp.class.getSimpleName() + "-jmh.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
