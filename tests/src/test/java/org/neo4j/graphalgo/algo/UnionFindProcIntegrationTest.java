package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.UnionFindProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class UnionFindProcIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Label)\n" +
                "CREATE (nB:Label)\n" +
                "CREATE (nC:Label)\n" +
                "CREATE (nD:Label)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" + // {J}
                "CREATE\n" +

                // {A, B, C, D}
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +

                "  (nD)-[:TYPE {cost:4.2}]->(nE),\n" + // threshold UF should split here

                // {E, F, G}
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

    @AfterClass
    public static void tearDown() throws Exception {
        if (db!=null) db.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testUnionFind() throws Exception {
        db.execute("CALL algo.unionFind('', '',{graph:'"+graphImpl+"'}) YIELD setCount")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWithLabel() throws Exception {
        db.execute("CALL algo.unionFind('Label', '',{graph:'"+graphImpl+"'}) YIELD setCount")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(1L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWriteBack() throws Exception {
        db.execute("CALL algo.unionFind('', 'TYPE', {write:true,graph:'"+graphImpl+"'}) YIELD setCount, writeMillis, nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return false;
                });
    }

    @Test
    public void testUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFind.stream('', 'TYPE', {graph:'"+graphImpl+"'}) YIELD setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
    }

    @Test
    public void testThresholdUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, threshold:5.0, graph:'"+graphImpl+"'}) YIELD setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 4, 3, 2, 1);
    }

    @Test
    public void testThresholdUnionFindLowThreshold() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute("CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, threshold:3.14, graph:'"+graphImpl+"'}) YIELD setId")
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
