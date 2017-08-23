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
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g"})
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GraphLoadLdbc {

    @Param({"LIGHT", "HEAVY"})
    GraphImpl graph;

    @Param({"true", "false"})
    boolean parallel;

    private GraphDatabaseAPI db;
    private GraphLoader loader;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();
        if (parallel) {
            loader = new GraphLoader(db, Pools.DEFAULT)
                    .withDirection(Direction.OUTGOING);
        } else {
            loader = new GraphLoader(db)
                    .withDirection(Direction.OUTGOING);
        }
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Graph run() throws Exception {
        return loader.load(this.graph.impl);
    }
}
