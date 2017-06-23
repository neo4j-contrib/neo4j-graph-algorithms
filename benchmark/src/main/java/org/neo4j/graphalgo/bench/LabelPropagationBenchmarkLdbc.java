package org.neo4j.graphalgo.bench;

import algo.Pools;
import algo.algo.LabelPropagation;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LabelPropagationBenchmarkLdbc {

    @Param({"1", "5"})
    int iterations;

    @Param({"10000", "100000000"})
    int batchSize;

    private HeavyGraph graph;
    private GraphDatabaseAPI db;
    private Transaction tx;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        procedures.registerProcedure(LabelPropagation.class);

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeProperty("partition", 0.0d)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
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
    public Object _01_algo() {
        return runPrintQuery(
                db,
                "CALL algo.labelPropagation(null, null, 'OUTGOING',{iterations:" + iterations + ",batchSize:" + batchSize + "}) "
                        + "YIELD loadMillis, computeMillis, writeMillis"
        );
    }

    @Benchmark
    public Object _02_apoc() {
        return runVoidQuery(
                db,
                "CALL algo.algo.community(" + iterations + ",[],'partition',null,'OUTGOING','weight'," + batchSize + ") "
                + "YIELD "
        );
    }

    @Benchmark
    public Object _03_direct() {
        return new org.neo4j.graphalgo.impl.LabelPropagation(graph, Pools.DEFAULT)
                .compute(Direction.OUTGOING, iterations, batchSize);
    }

    private static Object runPrintQuery(GraphDatabaseAPI db, String query) {
        return runQuery(db, query, result -> {
            long load = result.getNumber("loadMillis").longValue();
            long compute = result.getNumber("computeMillis").longValue();
            long write = result.getNumber("writeMillis").longValue();
            System.out.printf(
                    "load: %d ms%ncompute: %d ms%nwrite: %d ms%n",
                    load,
                    compute,
                    write);
        });
    }

    private static Object runVoidQuery(GraphDatabaseAPI db, String query) {
        return runQuery(db, query, r -> {});
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
