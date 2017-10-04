package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.PageRankResult;
import org.neo4j.graphalgo.impl.PageRankAlgorithm;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 3, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PageRankBenchmarkLdbc {

    @Param({"LIGHT", "HEAVY", "HUGE"})
    GraphImpl graph;

    @Param({"true", "false"})
    boolean parallel;

    @Param({"L01", "L10"})
    String graphId;;

    @Param({"5", "20"})
    int iterations;

    private GraphDatabaseAPI db;
    private Graph grph;
    private int batchSize;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb(graphId);
        grph = new GraphLoader(db, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withoutRelationshipWeights()
                .load(graph.impl);
        batchSize = parallel ? 10_000 : 2_000_000_000;
    }

    @TearDown
    public void shutdown() {
        grph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public PageRankResult run() throws Exception {
        return PageRankAlgorithm.of(
                grph,
                0.85,
                Pools.DEFAULT,
                Pools.getNoThreadsInDefaultPool(),
                batchSize)
                .compute(iterations)
                .result();
    }
}
