package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PageRankProcIntegrationTest {

    private static GraphDatabaseAPI db;
    private static Map<Long, Double> expected = new HashMap<>();

    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label1 {name:\"g\"})\n" +
            "CREATE (h:Label1 {name:\"h\"})\n" +
            "CREATE (i:Label1 {name:\"i\"})\n" +
            "CREATE (j:Label1 {name:\"j\"})\n" +
            "CREATE (k:Label2 {name:\"k\"})\n" +
            "CREATE (l:Label2 {name:\"l\"})\n" +
            "CREATE (m:Label2 {name:\"m\"})\n" +
            "CREATE (n:Label2 {name:\"n\"})\n" +
            "CREATE (o:Label2 {name:\"o\"})\n" +
            "CREATE (p:Label2 {name:\"p\"})\n" +
            "CREATE (q:Label2 {name:\"q\"})\n" +
            "CREATE (r:Label2 {name:\"r\"})\n" +
            "CREATE (s:Label2 {name:\"s\"})\n" +
            "CREATE (t:Label2 {name:\"t\"})\n" +
            "CREATE\n" +
            "  (b)-[:TYPE1]->(c),\n" +
            "  (c)-[:TYPE1]->(b),\n" +
            "  (d)-[:TYPE1]->(a),\n" +
            "  (d)-[:TYPE1]->(b),\n" +
            "  (e)-[:TYPE1]->(b),\n" +
            "  (e)-[:TYPE1]->(d),\n" +
            "  (e)-[:TYPE1]->(f),\n" +
            "  (f)-[:TYPE1]->(b),\n" +
            "  (f)-[:TYPE1]->(e),\n" +
            "  (g)-[:TYPE2]->(b),\n" +
            "  (g)-[:TYPE2]->(e),\n" +
            "  (h)-[:TYPE2]->(b),\n" +
            "  (h)-[:TYPE2]->(e),\n" +
            "  (i)-[:TYPE2]->(b),\n" +
            "  (i)-[:TYPE2]->(e),\n" +
            "  (j)-[:TYPE2]->(e),\n" +
            "  (k)-[:TYPE2]->(e)\n";

    @BeforeClass
    public static void setup() throws KernelException {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(PageRankProc.class);


        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Label1");
            expected.put(db.findNode(label, "name", "a").getId(), 0.0243);
            expected.put(db.findNode(label, "name", "b").getId(), 0.1968);
            expected.put(db.findNode(label, "name", "c").getId(), 0.1822);
            expected.put(db.findNode(label, "name", "d").getId(), 0.0219);
            expected.put(db.findNode(label, "name", "e").getId(), 0.0243);
            expected.put(db.findNode(label, "name", "f").getId(), 0.0219);
            expected.put(db.findNode(label, "name", "g").getId(), 0.0150);
            expected.put(db.findNode(label, "name", "h").getId(), 0.0150);
            expected.put(db.findNode(label, "name", "i").getId(), 0.0150);
            expected.put(db.findNode(label, "name", "j").getId(), 0.0150);
            tx.success();
        }
    }

    @Test
    public void testPageRankStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.pageRankStream('Label1', 'TYPE1') YIELD node, score",
                row -> actual.put(
                        row.getNode("node").getId(),
                        (Double) row.get("score")));

        assertMapEquals(expected, actual);
    }

    @Test
    public void testPageRankStatsAndDefaults() throws Exception {
        runQuery(
                "CALL algo.pageRankStats('Label1', 'TYPE1') YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, property",
                row -> {
                    assertEquals(20, row.getNumber("iterations").intValue());
                    assertEquals(
                            0.85,
                            row.getNumber("dampingFactor").doubleValue(),
                            1e-3);
                    assertFalse(row.getBoolean("write"));
                    assertNull(row.getString("property"));

                    assertEquals(-1, row.getNumber("writeMillis").intValue());
                    assertTrue(
                            "load time not set",
                            row.getNumber("loadMillis").intValue() >= 0);
                    assertTrue(
                            "compute time not set",
                            row.getNumber("computeMillis").intValue() >= 0);

                    assertEquals(
                            expected.size(),
                            row.getNumber("nodes").intValue());
                });
    }

    @Test
    public void testPageRankStatsAndParameters() throws Exception {
        runQuery(
                "CALL algo.pageRankStats('Label1', 'TYPE1',{iterations:5,dampingFactor:0.42})",
                row -> {
                    assertEquals(5, row.getNumber("iterations").intValue());
                    assertEquals(
                            0.42,
                            row.getNumber("dampingFactor").doubleValue(),
                            1e-3);
                });

    }
    @Test
    public void testPageRankStatsDisabledWrite() throws Exception {
        runQuery(
                "CALL algo.pageRankStats('Label1', 'TYPE1')",
                row -> assertFalse(row.getBoolean("write")));
    }

    @Test
    public void testPageRankWriteBack() throws Exception {
        runQuery(
                "CALL algo.pageRank('Label1', 'TYPE1') YIELD writeMillis, write, property",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("score", row.getString("property"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
                        .getNodeById(entry.getKey())
                        .getProperty("score")).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        1e-4);
            }
            tx.success();
        }
    }

    @Test
    public void testPageRankWriteBackUnderDifferentProperty() throws Exception {
        runQuery(
                "CALL algo.pageRank('Label1', 'TYPE1', {scoreProperty:'foobar'}) YIELD writeMillis, write, property",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("foobar", row.getString("property"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
                        .getNodeById(entry.getKey())
                        .getProperty("foobar")).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        1e-4);
            }
            tx.success();
        }
    }

    private static void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    private static void assertMapEquals(
            Map<Long, Double> expected,
            Map<Long, Double> actual) {
        assertEquals("number of elements", expected.size(), actual.size());
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    expected.get(entry.getKey()),
                    entry.getValue(),
                    1e-4);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
