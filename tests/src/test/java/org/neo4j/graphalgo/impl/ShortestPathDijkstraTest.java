package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public final class ShortestPathDijkstraTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{LightGraphFactory.class, "LightGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
        );
    }

    // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE (g:Label2 {name:\"g\"})\n" +
            "CREATE (h:Label2 {name:\"h\"})\n" +
            "CREATE (i:Label2 {name:\"i\"})\n" +
            "CREATE (j:Label2 {name:\"j\"})\n" +
            "CREATE (k:Label2 {name:\"k\"})\n" +
            "CREATE (l:Label2 {name:\"l\"})\n" +
            "CREATE\n" +
            "  (a)-[:TYPE1 {cost:4}]->(b),\n" +
            "  (a)-[:TYPE1 {cost:2}]->(c),\n" +
            "  (b)-[:TYPE1 {cost:5}]->(c),\n" +
            "  (b)-[:TYPE1 {cost:10}]->(d),\n" +
            "  (c)-[:TYPE1 {cost:3}]->(e),\n" +
            "  (d)-[:TYPE1 {cost:11}]->(f),\n" +
            "  (e)-[:TYPE1 {cost:4}]->(d),\n" +
            "  (a)-[:TYPE2 {cost:1}]->(d),\n" +
            "  (b)-[:TYPE2 {cost:1}]->(f)\n";

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setupGraph() {
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        db.shutdown();
    }

    public ShortestPathDijkstraTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() throws Exception {
        final Label label = Label.label("Label1");

        final long[] expected;
        try (Transaction tx = db.beginTx()) {
            expected = new long[]{
                    db.findNode(label, "name", "a").getId(),
                    db.findNode(label, "name", "c").getId(),
                    db.findNode(label, "name", "e").getId(),
                    db.findNode(label, "name", "d").getId(),
                    db.findNode(label, "name", "f").getId()
            };
            tx.success();
        }

        final Graph graph = new GraphLoader(db)
                .withLabel(label)
                .withRelationshipType("TYPE1")
                .withWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(graphImpl);

        final int[] path = new ShortestPathDijkstra(graph).compute(
                expected[0],
                expected[expected.length - 1]
        );
        final long[] actual = Arrays
                .stream(path)
                .mapToLong(graph::toOriginalNodeId)
                .toArray();

        assertArrayEquals(
                expected,
                actual
        );
    }
}
