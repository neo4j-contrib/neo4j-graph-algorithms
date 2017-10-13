package org.neo4j.graphalgo.impl;

import org.junit.*;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.lightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.io.File;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 *
 * @author mknblch
 */
@Ignore("depends on local data")
public class BetweennessCentralityTest_349 {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    private static final String PATH = "file:///Users/mknobloch/repository/neo4j-graph-algorithms/tests/src/test/resources/facebook_combined.txt";

    public static final String GRAPH_DIRECTORY = "/tmp/facebook_graph.db";

    private static File storeDir = new File(GRAPH_DIRECTORY);

    @BeforeClass
    public static void setup() throws Exception {

        final boolean exists = storeDir.exists();

        if (null != db) {
            db.shutdown();
        }

        db = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(storeDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory, "4G")
                .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);

        if (!exists) {
            try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
                db.execute(String.format("LOAD CSV FROM '%s' as row fieldterminator ' '\n", PATH) +
                        "MERGE (u:User{id:row[0]})\n" +
                        "MERGE (u1:User{id:row[1]})\n" +
                        "MERGE (u)-[:FRIEND]-(u1)\n");

            }
        } else {
            System.out.println("using " + storeDir.getAbsolutePath());
        }

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        graph = null;
    }

    @Test
    public void test() throws Exception {

        final String importCypher = "CALL algo.betweenness('User', 'FRIEND', " +
                "{direction:'both',write:true, stats:true, writeProperty:'centrality',concurrency: 4}) \n" +
                "YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";

        db.execute(importCypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final double minCentrality = row.getNumber("minCentrality").doubleValue();
            final double maxCentrality = row.getNumber("maxCentrality").doubleValue();
            final double sumCentrality = row.getNumber("sumCentrality").doubleValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();

            System.out.println("nodes = " + nodes);
            System.out.println("minCentrality = " + minCentrality);
            System.out.println("maxCentrality = " + maxCentrality);
            System.out.println("sumCentrality = " + sumCentrality);
            System.out.println("loadMillis = " + loadMillis);
            System.out.println("computeMillis = " + computeMillis);
            System.out.println("writeMillis = " + writeMillis);

            return true;
        });

        final String checkCypher = "MATCH (n:User) RETURN id(n) as id, n.centrality as c";

        db.execute(checkCypher).accept(row -> {
            final long id = row.getNumber("id").longValue();
            final double c = row.getNumber("c").doubleValue();
            assertTrue(c >= 0.0);
            return true;
        });
    }

    @Test
    public void testDirect() throws Exception {

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withLabel("User")
                .withRelationshipType("FRIEND")
                .load(HeavyGraphFactory.class);

        final double[] centrality = new BetweennessCentrality(graph)
                .compute()
                .getCentrality();

        assertEquals(graph.nodeCount(), centrality.length);

        for (int i = 0; i < centrality.length; i++) {
//            System.out.printf("%4d : %9.2f%n", i, centrality[i]);
            assertTrue(centrality[i] >= 0.0);
        }
    }


    @Test
    public void testDirectParallel() throws Exception {

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withLabel("User")
                .withRelationshipType("FRIEND")
                .load(HeavyGraphFactory.class);

        final Optional<BetweennessCentrality.Result> any = new ParallelBetweennessCentrality(graph, 100_000, Pools.DEFAULT, 4)
                .withDirection(Direction.BOTH)
                .compute()
                .resultStream()
                .filter(r -> r.centrality < 0)
                .findAny();

        if (any.isPresent()) {
            fail("negative values");
        }
    }
}
