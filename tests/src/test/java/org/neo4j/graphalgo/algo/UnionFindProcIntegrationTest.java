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
import static org.junit.Assert.assertNotEquals;
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
                "CREATE\n" +

                // {A, B, C, D}
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +

                "  (nD)-[:TYPE {cost:4.2}]->(nE),\n" + // threshold UF should split here

                // {E, G, G}
                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +

                // {H, I}
                "  (nH)-[:TYPE]->(nI)";



        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(UnionFindProc.class);
    }

    @Test
    public void testUnionFind() throws Exception {
        db.execute("CALL algo.unionFind('', 'TYPE') YIELD setCount, loadDuration, evalDuration, writeDuration")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(2L, row.getNumber("setCount"));
                    return false;
                });
    }

    @Test
    public void testUnionFindWriteBack() throws Exception {
        db.execute("CALL algo.unionFind('', 'TYPE', {write:true}) YIELD setCount, loadDuration, evalDuration, writeDuration")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeDuration"));
                    assertEquals(2L, row.getNumber("setCount"));
                    return false;
                });
    }

    @Test
    public void testUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFindStream('', 'TYPE', {}) YIELD nodeId, setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
    }

    @Test
    public void testThresholdUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFindStream('', 'TYPE', {property:'cost', defaultValue:10.0, threshold:5.0}) YIELD nodeId, setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 4, 3, 2, 1);
    }

    @Test
    public void testThresholdUnionFindLowThreshold() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFindStream('', 'TYPE', {property:'cost', defaultValue:10.0, threshold:3.14}) YIELD nodeId, setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
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
