package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.procedures.IntProcedure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.Assert.assertEquals;

/**
 * expected path OUTGOING:  abcf
 *               INCOMING:  adef
 *               BOTH:      adef
 *
 * x should be unreachable
 *     2    2   2
 *   ,->(b)->(c)->(f)
 *  |  1    1    1 |   (x) // unreachable
 * (a)<-(d)<-(e)<-´
 *
 * @author mknblch
 */
public class DirectedDijkstraSPTest {

    public static final String cypher =
        "CREATE (d:Node {name:'d'})\n" +
        "CREATE (a:Node {name:'a'})\n" +
        "CREATE (c:Node {name:'c'})\n" +
        "CREATE (b:Node {name:'b'})\n" +
        "CREATE (f:Node {name:'f'})\n" +
        "CREATE (e:Node {name:'e'})\n" +
        "CREATE (x:Node {name:'x'})\n" +

        "CREATE\n" +
            "  (a)-[:REL {cost:2}]->(b),\n" +
            "  (b)-[:REL {cost:2}]->(c),\n" +
            "  (c)-[:REL {cost:2}]->(f),\n" +
            "  (f)-[:REL {cost:1}]->(e),\n" +
            "  (e)-[:REL {cost:1}]->(d),\n" +
            "  (d)-[:REL {cost:1}]->(a)\n";

    private static GraphDatabaseAPI api;
    private static Graph graph;

    @BeforeClass
    public static void setup() {

        api = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(api)
                .withNodeStatement("Node")
                .withRelationshipType("REL")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void tearDown() {
        api.shutdown();
    }


    private static long id(String name) {
        try (Transaction transaction = api.beginTx()) {
            return api.findNode(Label.label("Node"), "name", name).getId();
        }
    }

    private static String name(long id) {
        final String[] name = {""};
        api.execute(String.format("MATCH (n:Node) WHERE id(n)=%d RETURN n.name as name", id)).accept(row -> {
            name[0] = row.getString("name");
            return false;
        });
        return name[0];
    }

    @Test
    public void testOutgoing() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("f"), Direction.OUTGOING);

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(OUTGOING) = " + path);
        assertEquals("abcf", path.toString());
        assertEquals(6.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    public void testIncoming() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("f"), Direction.INCOMING);

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(INCOMING) = " + path);
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    public void testBoth() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("f")); // default is both

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(BOTH) = " + path);
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    public void testUnreachableOutgoing() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("x"), Direction.OUTGOING); // default is both

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    public void testUnreachableIncoming() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("x"), Direction.INCOMING); // default is both

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    public void testUnreachableBoth() throws Exception {

        final StringBuilder path = new StringBuilder();
        final ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph)
                .compute(id("a"), id("x"), Direction.BOTH); // default is both

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }
}
