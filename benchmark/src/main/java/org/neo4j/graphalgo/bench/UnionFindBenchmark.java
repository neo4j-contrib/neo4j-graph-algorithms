package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class UnionFindBenchmark {

    @Param({"LIGHT_QUEUE", "LIGHT_FORK_JOIN", "LIGHT_FJ_MERGE", "LIGHT_SEQ", "HEAVY_QUEUE", "HEAVY_FORK_JOIN", "HEAVY_FJ_MERGE", "HEAVY_SEQ", "HUGE_QUEUE", "HUGE_FORK_JOIN", "HUGE_FJ_MERGE", "HUGE_SEQ", "HUGE_HUGE_QUEUE", "HUGE_HUGE_FORK_JOIN", "HUGE_HUGE_FJ_MERGE", "HUGE_HUGE_SEQ"})
    UFBenchmarkCombination uf;

    private Graph theGraph;

    @Setup
    public void setup(HeroGraph heroGraph) {
        theGraph = new GraphLoader(heroGraph.db).load(uf.graph.impl);
        heroGraph.db.shutdown();
    }

    @TearDown
    public void tearDown() {
        theGraph.release();
        Pools.DEFAULT.shutdownNow();
    }


    @Benchmark
    public Object unionFind() {
        return uf.run(theGraph);
    }
}
