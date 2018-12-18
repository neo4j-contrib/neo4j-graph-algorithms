package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.MatchingAlgoProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AnalysisAlgoProcTest {
    private static final RelationshipType type = RelationshipType.withName("TYPE");
    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        String cypher = "CREATE(a:Node) SET a.total_distance = 100 " +
                "CREATE(b:Node) SET b.total_distance = 200 " +
                "CREATE(c:Node) SET c.total_distance = 300 " +
                "CREATE(d:Node) SET d.total_distance = 150 " +
                "CREATE(e:Node) SET e.total_distance = 250 " +
                // group 1
                "CREATE (a)-[:TYPE]->(b) " +
                "CREATE (a)<-[:TYPE]-(b) " +
                "CREATE (a)-[:TYPE]->(c) " +
                "CREATE (a)<-[:TYPE]-(c) " +
                "CREATE (b)-[:TYPE]->(c) " +
                "CREATE (b)<-[:TYPE]-(c) " +
                // group 2
                "CREATE (d)-[:TYPE]->(e) " +
                "CREATE (d)<-[:TYPE]-(e) " ;

        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(MatchingAlgoProc.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testMatchingAlgoCapacity() throws Exception {

        db.execute("MATCH (node:Node) " +
                "CALL fk.algo.matching_algo_capacity(node, node, node,'ROUTE', 'distance' ) " +
                "YIELD loadUuid, offRoute, deadHead, loadMillis, evalMillis " +
                "RETURN loadUuid, offRoute, deadHead, loadMillis, evalMillis")
                .accept(row -> {

                    System.out.println(row.getNumber("loadMillis").longValue());
                    System.out.println(row.getNumber("evalMillis").longValue());

                    return true;
                });
    }
}
