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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class PageRankWikiTest {

    private Class<? extends GraphFactory> graphImpl;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "HeavyGraphFactory"},
                new Object[]{LightGraphFactory.class, "LightGraphFactory"},
                new Object[]{GraphViewFactory.class, "GraphViewFactory"}
        );
    }

    private static final String DB_CYPHER = "" +
            "CREATE (a:Node {name:\"a\"})\n" +
            "CREATE (b:Node {name:\"b\"})\n" +
            "CREATE (c:Node {name:\"c\"})\n" +
            "CREATE (d:Node {name:\"d\"})\n" +
            "CREATE (e:Node {name:\"e\"})\n" +
            "CREATE (f:Node {name:\"f\"})\n" +
            "CREATE (g:Node {name:\"g\"})\n" +
            "CREATE (h:Node {name:\"h\"})\n" +
            "CREATE (i:Node {name:\"i\"})\n" +
            "CREATE (j:Node {name:\"j\"})\n" +
            "CREATE (k:Node {name:\"k\"})\n" +
            "CREATE\n" +
            // a (dangling node)
            // b
            "  (b)-[:TYPE]->(c),\n" +
            // c
            "  (c)-[:TYPE]->(b),\n" +
            // d
            "  (d)-[:TYPE]->(a),\n" +
            "  (d)-[:TYPE]->(b),\n" +
            // e
            "  (e)-[:TYPE]->(b),\n" +
            "  (e)-[:TYPE]->(d),\n" +
            "  (e)-[:TYPE]->(f),\n" +
            // f
            "  (f)-[:TYPE]->(b),\n" +
            "  (f)-[:TYPE]->(e),\n" +
            // g
            "  (g)-[:TYPE]->(b),\n" +
            "  (g)-[:TYPE]->(e),\n" +
            // h
            "  (h)-[:TYPE]->(b),\n" +
            "  (h)-[:TYPE]->(e),\n" +
            // i
            "  (i)-[:TYPE]->(b),\n" +
            "  (i)-[:TYPE]->(e),\n" +
            // j
            "  (j)-[:TYPE]->(e),\n" +
            // k
            "  (k)-[:TYPE]->(e)\n";

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

    public PageRankWikiTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test() throws Exception {
        final Label label = Label.label("Node");
        final Map<Long, Double> expected = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            expected.put(db.findNode(label, "name", "a").getId(), 0.304);
            expected.put(db.findNode(label, "name", "b").getId(), 3.560);
            expected.put(db.findNode(label, "name", "c").getId(), 3.175);
            expected.put(db.findNode(label, "name", "d").getId(), 0.362);
            expected.put(db.findNode(label, "name", "e").getId(), 0.750);
            expected.put(db.findNode(label, "name", "f").getId(), 0.362);
            expected.put(db.findNode(label, "name", "g").getId(), 0.150);
            expected.put(db.findNode(label, "name", "h").getId(), 0.150);
            expected.put(db.findNode(label, "name", "i").getId(), 0.150);
            expected.put(db.findNode(label, "name", "j").getId(), 0.150);
            expected.put(db.findNode(label, "name", "k").getId(), 0.150);
            tx.close();
        }

        final Graph graph = new GraphLoader(db)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .load(graphImpl);

        final double[] ranks = new PageRank(graph, graph, graph, graph, 0.85).compute(40).getPageRank();
        System.out.println("ranks = " + Arrays.toString(ranks));
        IntStream.range(0, ranks.length).forEach(i -> {
            final long nodeId = graph.toOriginalNodeId(i);
            assertEquals(
                    "Node#" + nodeId,
                    expected.get(nodeId),
                    ranks[i],
                    0.001
            );
        });
    }
}
