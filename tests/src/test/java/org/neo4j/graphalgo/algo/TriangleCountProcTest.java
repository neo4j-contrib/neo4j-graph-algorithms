package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.exporter.TriangleCountProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class TriangleCountProcTest {


    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(f),\n" +
                        " (f)-[:TYPE]->(d),\n" +

                        " (a)-[:TYPE]->(d),\n" +

                        " (g)-[:TYPE]->(h),\n" +
                        " (h)-[:TYPE]->(i),\n" +
                        " (i)-[:TYPE]->(g)";

        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(TriangleCountProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void testCypher() throws Exception {

        final String cypher = "CALL algo.triangleCount({labelOrQuery:'Node', relationshipQuery:''}) YIELD loadMillis, computeMillis, nodeCount, triangleCount";

        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
            return true;
        });


    }

}
