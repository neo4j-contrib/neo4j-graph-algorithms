package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.UnionFindProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
public class UnionFindProcIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph = "CREATE (nA)\n" +
                "CREATE (nB)\n" +
                "CREATE (nC)\n" +
                "CREATE (nD)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" +
                "CREATE (nK)\n" +
                "CREATE\n" +
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +
                "  (nD)-[:TYPE]->(nA),\n" +

                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +
                "  (nG)-[:TYPE]->(nH),\n" +
                "  (nH)-[:TYPE]->(nI),\n" +
                "  (nI)-[:TYPE]->(nE),\n" +

                "  (nJ)-[:TYPE]->(nK),\n" +
                "  (nK)-[:TYPE]->(nJ);";

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);

        proceduresService.registerProcedure(UnionFindProc.class);
    }

    @Test
    public void testSimpleUF() throws Exception {

        final IntIntScatterMap map = new IntIntScatterMap(11);

        db.execute("CALL org.neo4j.graphalgo.unionFind(null, 'TYPE') YIELD nodeId, setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(((Long) row.get("setId")).intValue(), 1);
                    return true;
                });

        assertMapContains(map, 4, 5, 2);
    }

    private static void assertMapContains(IntIntMap map, int... values) {
        assertEquals("set count does not match", values.length, map.size());
        for (int count : values) {
            assertTrue("set size " + count + " does not match", mapContainsValue(map, count));
        }
    }

    private static boolean mapContainsValue(IntIntMap map, int value) {
        for (IntIntCursor cursor : map) {
            if (cursor.value == value) {
                return true;
            }
        }
        return false;
    }
}
