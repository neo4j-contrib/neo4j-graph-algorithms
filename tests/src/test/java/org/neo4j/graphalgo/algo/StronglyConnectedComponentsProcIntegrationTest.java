package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class StronglyConnectedComponentsProcIntegrationTest {

    private static final RelationshipType type = RelationshipType.withName("TYPE");
    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        String cypher = "CREATE(a:Node) " +
                "CREATE(b:Node) " +
                "CREATE(c:Node) " +
                "CREATE(d:Node) " +
                "CREATE(e:Node) " +
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

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try(Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(StronglyConnectedComponentsProc.class);
    }
    @AfterClass
    public static void tearDown() throws Exception {
        if (db!=null) db.shutdown();
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
    public void testScc() throws Exception {

        db.execute("CALL algo.scc('Node', 'TYPE', {write:true, graph:'"+graphImpl+"'}) YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize")
                .accept(row -> {

                    System.out.println(row.getNumber("loadMillis").longValue());
                    System.out.println(row.getNumber("computeMillis").longValue());
                    System.out.println(row.getNumber("writeMillis").longValue());
                    System.out.println(row.getNumber("setCount").longValue());
                    System.out.println(row.getNumber("maxSetSize").longValue());
                    System.out.println(row.getNumber("minSetSize").longValue());

                    assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                    assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                    assertEquals(2, row.getNumber("setCount").longValue());
                    assertEquals(2, row.getNumber("minSetSize").longValue());
                    assertEquals(3, row.getNumber("maxSetSize").longValue());

                    return true;
                });
    }
}
