package org.neo4j.graphalgo.bench;

import algo.Pools;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.GraphUnionFind;
import org.neo4j.graphalgo.impl.ParallelUnionFind;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = "-Xmx4G")
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

    @Setup
    public static void setup() throws Exception {

        final File storeDir = new File("/tmp/graph.db" + (int)(Math.random() * 10000));
//        if (storeDir.exists()) {
//            FileUtils.deleteFile(storeDir);
//        }

        db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory, "8G")
                .newGraphDatabase();

        bridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph(20, 100_000);
        }

        db.execute("MATCH (n) RETURN n");

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }


    private static void createTestGraph(int sets, int setSize) throws Exception {
        final int rIdx;
        try (Transaction tx = db.beginTx();
             Statement stm = bridge.get()) {
            DataWriteOperations op = stm.dataWriteOperations();
            rIdx = op.relationshipTypeGetOrCreateForName(RELATIONSHIP_TYPE.name());
            tx.success();
        }

        final ArrayList<Runnable> runnables = new ArrayList<Runnable>();
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
    public Object _02_parallelUnionFind_400000() {
        return new ParallelUnionFind(graph, Pools.DEFAULT, 100_000)
                .compute()
                .getStruct();
    }

    @Benchmark
    public Object _03_sequentialUnionFind() {
        return new GraphUnionFind(graph)
                .compute();

    }

}
