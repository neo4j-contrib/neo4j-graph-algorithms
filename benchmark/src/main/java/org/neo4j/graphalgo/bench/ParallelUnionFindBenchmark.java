package org.neo4j.graphalgo.bench;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.*;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = "-Xms4G")
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ParallelUnionFindBenchmark {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;

    private static Graph graph;

    private static ThreadToStatementContextBridge bridge;

    public static final String GRAPH_DIRECTORY = "/tmp/graph.db";

    private static File storeDir = new File(GRAPH_DIRECTORY);

//    @Param({"5", "10", "20", "50"})
    public static int numSets = 20;

    @Setup
    public static void setup() throws Exception {

        FileUtils.deleteRecursively(storeDir);
        if (storeDir.exists()) {
            throw new IllegalStateException("could not delete " + storeDir);
        }

        if (null != db) {
            db.shutdown();
        }

        db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory, "4G")
                .newGraphDatabase();

        bridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph(numSets, 100_000);
        }

        db.execute("MATCH (n) RETURN n");

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);
    }

    @TearDown
    public void shutdown() throws IOException {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();

        FileUtils.deleteRecursively(storeDir);
        if (storeDir.exists()) {
            throw new IllegalStateException("Could not delete graph");
        }
    }


    private static void createTestGraph(int sets, int setSize) throws Exception {
        final int rIdx;
        try (Transaction tx = db.beginTx();
             Statement stm = bridge.get()) {
            DataWriteOperations op = stm.dataWriteOperations();
            rIdx = op.relationshipTypeGetOrCreateForName(RELATIONSHIP_TYPE.name());
            tx.success();
        }

        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < sets; i++) {
            runnables.add(createLine(setSize, rIdx));
        }
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    private static Runnable createLine(int size, int rIdx) throws Exception{
        return () -> {
            try (Transaction tx = db.beginTx();
                 Statement stm = bridge.get()) {
                final DataWriteOperations op = stm.dataWriteOperations();
                long node = op.nodeCreate();
                for (int i = 1; i < size; i++) {
                    final long temp = op.nodeCreate();
                    try {
                        op.relationshipCreate(rIdx, node, temp);
                    } catch (RelationshipTypeIdNotFoundKernelException | EntityNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    node = temp;
                }
                tx.success();
            } catch (InvalidTransactionTypeKernelException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Benchmark
    public Object parallelUnionFindQueue_200000() {
        return new ParallelUnionFindQueue(graph, Pools.DEFAULT, 200_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindQueue_400000() {
        return new ParallelUnionFindQueue(graph, Pools.DEFAULT, 400_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindQueue_800000() {
        return new ParallelUnionFindQueue(graph, Pools.DEFAULT, 800_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindForkJoinMerge_400000() {
        return new ParallelUnionFindFJMerge(graph, Pools.DEFAULT, 400_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindForkJoinMerge_800000() {
        return new ParallelUnionFindFJMerge(graph, Pools.DEFAULT, 800_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindForkJoin_400000() {
        return new ParallelUnionFindForkJoin(graph, Pools.DEFAULT, 400_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object parallelUnionFindForkJoin_800000() {
        return new ParallelUnionFindForkJoin(graph, Pools.DEFAULT, 800_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object sequentialUnionFind() {
        return new GraphUnionFind(graph)
                .compute();

    }

}
