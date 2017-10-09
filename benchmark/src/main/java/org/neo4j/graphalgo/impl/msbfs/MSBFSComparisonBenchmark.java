package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MSBFSComparisonBenchmark {

    @Param({
            "_1024_32",
            "_1024_128",
            "_1024_1024",
            "_1024",
            "_8192_32",
            "_8192_128",
//            "_8192_1024",
//            "_8192_8192",
//            "_8192",
//            "_16384_32",
//            "_16384_128",
//            "_16384_1024",
//            "_16384_8192",
//            "_16384_16384",
//            "_16384"
    })
    public MSBFSSource source;

    @TearDown
    public void shutdown() {
        Pools.DEFAULT.shutdown();
    }

    @Benchmark
    public MsBFSAlgo _01_normal(Blackhole bh) throws Throwable {
        MultiSourceBFS msbfs = new MultiSourceBFS(
                source.nodes,
                source.rels,
                Direction.OUTGOING,
                consume(bh),
                source.sources);
        return measure(msbfs);
    }

    @Benchmark
    public MsBFSAlgo _02_huge(Blackhole bh) throws Throwable {
        HugeMultiSourceBFS msbfs = new HugeMultiSourceBFS(
                source.hugeNodes,
                source.hugeRels,
                Direction.OUTGOING,
                hugeConsume(bh),
                AllocationTracker.EMPTY,
                source.hugeSources);
        return measure(msbfs);
    }

    private MsBFSAlgo measure(MsBFSAlgo msbfs) throws Throwable {
        try {
            msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT);
        } catch (StackOverflowError e) {
            Throwable error = e;
            Pools.DEFAULT.shutdownNow();
            try {
                Pools.DEFAULT.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e1) {
                e1.addSuppressed(e);
                error = e1;
            }
            throw error;
        }
        return msbfs;
    }

    private static BfsConsumer consume(Blackhole bh) {
        return (i, d, s) -> bh.consume(i);
    }

    private static HugeBfsConsumer hugeConsume(Blackhole bh) {
        return (i, d, s) -> bh.consume(i);
    }
}
