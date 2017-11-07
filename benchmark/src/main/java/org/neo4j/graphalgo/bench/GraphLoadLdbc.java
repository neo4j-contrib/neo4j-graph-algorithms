package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 3, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GraphLoadLdbc {

    @Param({"LIGHT", "HEAVY", "HUGE"})
    GraphImpl graph;

    @Param({"true", "false"})
    boolean parallel;

    @Param({"IN", "OUT", "NONE", "BOTH"})
    DirectionParam dir;

    @Param({"", "length"})
    String weightProp;

    @Param({"", "Person"})
    String label;

    @Param({"L01", "L10"})
    String graphId;

    private GraphDatabaseAPI db;
    private GraphLoader loader;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb(graphId);
        loader = new GraphLoader(db)
                .withOptionalRelationshipWeightsFromProperty(weightProp.isEmpty() ? null : weightProp, 1.0)
                .withOptionalLabel(label.isEmpty() ? null : label)
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
    public void load(Blackhole bh) throws Exception {
        Graph graph = loader.load(this.graph.impl);
        bh.consume(graph);
        graph.release();
        bh.consume(loader);
    }
}
