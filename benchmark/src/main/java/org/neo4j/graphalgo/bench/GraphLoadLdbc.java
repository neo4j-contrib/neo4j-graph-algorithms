package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
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
@Fork(value = 3, jvmArgs = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GraphLoadLdbc {

    @Param({"LIGHT", "HEAVY", "HUGE", "VIEW"})
    GraphImpl graph;

    @Param({"true", "false"})
    boolean parallel;

    @Param({"8", "64"})
    int concurrency;

    @Param({"IN", "OUT", "NONE", "BOTH"})
    DirectionParam dir;

    @Param({"", "length"})
    String weightProp;

    private GraphDatabaseAPI db;
    private GraphLoader loader;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();
        loader = new GraphLoader(db)
                .withOptionalRelationshipWeightsFromProperty(weightProp.isEmpty() ? null : weightProp, 1.0)
                .withConcurrency(concurrency)
                .withDirection(dir.direction);
        if (parallel) {
            loader.withExecutorService(Pools.DEFAULT);
        }
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Graph load() throws Exception {
        return loader.load(this.graph.impl);
    }
}
