package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.ClosenessCentralityProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.impl.MSClosenessCentrality;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertNotEquals;


/**
 * Test for <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/291">Issue 291</a>
 *
 * @author mknblch
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class ClosenessCentralityIntegrationTest_291 {

    public static final String URL = "https://raw.githubusercontent.com/mathbeveridge/asoiaf/master/data/asoiaf-all-edges.csv";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ClosenessCentralityProc.class);

        final String importQuery = String.format("LOAD CSV WITH HEADERS FROM '%s' AS row\n" +
                "MERGE (src:Character {name: row.Source})\n" +
                "MERGE (tgt:Character {name: row.Target})\n" +
                "MERGE (src)-[r:INTERACTS]->(tgt) ON CREATE SET r.weight = toInteger(row.weight)", URL);

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Setup took %d ms%n", l))) {
            try (Transaction tx = db.beginTx()) {
                db.execute(importQuery);
                tx.success();
            }
        }

    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @Test
    public void testDirect() throws Exception {

        final Graph graph = new GraphLoader(db, Pools.DEFAULT)
                .withLabel("Character")
                .withRelationshipType("INTERACTS")
                .withoutNodeWeights()
                .withoutRelationshipWeights()
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);

        final MSClosenessCentrality algo = new MSClosenessCentrality(graph, 4, Pools.DEFAULT)
                .compute();

        final AtomicIntegerArray farness = algo.getFarness();
        final double[] centrality = algo.getCentrality();

        System.out.println("graph.nodeCount() = " + graph.nodeCount());

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.printf("node %3d | farness %3d | closeness %3f%n",
                    i,
                    farness.get(i),
                    centrality[i]);
        }
    }

    @Test
    public void test() throws Exception {
        final String callQuery = "CALL algo.closeness('Character', 'INTERACTS', " +
                "{write:true, writeProperty:'closeness', stats:true, concurrency:8})";
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Algorithm took %d ms%n", l))) {
            db.execute(callQuery);
        }

        final String testQuery = "MATCH (c:Character) WHERE exists(c.closeness) RETURN c.name as name, " +
                "c.closeness as closeness ORDER BY closeness DESC LIMIT 50";
        db.execute(testQuery).accept(row -> {
            final String name = row.getString("name");
            final double closeness = row.getNumber("closeness").doubleValue();
            System.out.printf("%s = %3f%n", name, closeness);
            return true;
        });
    }
}
