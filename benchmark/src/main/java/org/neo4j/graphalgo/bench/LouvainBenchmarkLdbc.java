package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g", "-XX:+UseG1GC"})
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class LouvainBenchmarkLdbc {

    private GraphDatabaseAPI db;
    private Transaction tx;


    @Param({"4", "8"})
    int threads;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LouvainProc.class);
    }

    @Setup(Level.Invocation)
    public void startTx() {
        tx = db.beginTx();
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @TearDown(Level.Invocation)
    public void failTx() {
        tx.failure();
        tx.close();
    }

    @Benchmark
    public Object _01_louvainParallel() {
        return runQuery(
                db,
                "CALL algo.louvain(null, null, {maxIterations:1, concurrency:" + threads + "}) "
                        + "YIELD loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations"
                , r -> {
                    long load = r.getNumber("loadMillis").longValue();
                    long compute = r.getNumber("computeMillis").longValue();
                    long write = r.getNumber("writeMillis").longValue();
                    long count = r.getNumber("communityCount").longValue();
                    long iter = r.getNumber("iterations").longValue();
                    long nodes = r.getNumber("nodes").longValue();

                    System.out.println("communities = " + count);
                    System.out.println("iter = " + iter);
                    System.out.println("nodes = " + nodes);
                    System.out.println("load = " + load);
                    System.out.println("compute = " + compute);
                    System.out.println("write = " + write);

                }
        );
    }

    private static Object runQuery(
            GraphDatabaseAPI db,
            String query,
            Consumer<Result.ResultRow> action) {
        try (Result result = db.execute(query)) {
            result.accept(r -> {
                action.accept(r);
                return true;
            });
        }
        return db;
    }
}
