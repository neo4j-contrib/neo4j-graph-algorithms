package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;


/**         5     5      5
 *      (A)---(B)---(C)----.
 *    5/ 2    2     2     2 \
 *  (S)---(G)---(H)---(I)---(X)
 *    3\    3     3     3   /
 *      (D)---(E)---(F)----°
 *
 * S->X: {G,H,I:8}, {D,E,F:12}, {A,B,C:20}
 *
 */
public final class ShortestPathDeltaSteppingTest {

    private static GraphDatabaseAPI api;

    private static Graph graph;

    private static Node head;

//    @BeforeClass
//    public static void setup() {
//
//
//        api = (GraphDatabaseAPI)
//                new TestGraphDatabaseFactory()
//                        .newImpermanentDatabaseBuilder()
//                        .newGraphDatabase();
//
//        DefaultBuilder builder = GridBuilder.create(api)
//                .setRelationship("TYPE")
//                .setLabel("Node");
//
//        head = builder.createNode();
//
//        builder.newLineBuilder()
//                .withHead(head)
//                .createLine(4).forEachRelInTx(r -> r.setProperty("cost", 2.0))
//                .newLineBuilder()
//                .withHead(head)
//                .createLine(4).forEachRelInTx(r -> r.setProperty("cost", 3.0))
//                .newLineBuilder()
//                .withHead(head)
//                .createLine(4).forEachRelInTx(r -> r.setProperty("cost", 5.0));
//
//        graph = new GraphLoader(api)
//                .withLabel("Node")
//                .withRelationshipType("TYPE")
//                .withRelationshipWeightsFromProperty("cost", 9999)
//                .load(LightGraphFactory.class);
//    }

    @BeforeClass
    public static void setup() {
        final String cypher =
                "CREATE (s:Node {start:true})\n" +
                        "CREATE (a:Node)\n" +
                        "CREATE (b:Node)\n" +
                        "CREATE (c:Node)\n" +
                        "CREATE (d:Node)\n" +
                        "CREATE (e:Node)\n" +
                        "CREATE (f:Node)\n" +
                        "CREATE (g:Node)\n" +
                        "CREATE (h:Node)\n" +
                        "CREATE (i:Node)\n" +
                        "CREATE (x:Node)\n" +
                        "CREATE" +
                        " (s)-[:TYPE {cost:5}]->(a),\n" +
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(x),\n" +

                        " (s)-[:TYPE {cost:3}]->(d),\n" +
                        " (d)-[:TYPE {cost:3}]->(e),\n" +
                        " (e)-[:TYPE {cost:3}]->(f),\n" +
                        " (f)-[:TYPE {cost:3}]->(x),\n" +

                        " (s)-[:TYPE {cost:2}]->(g),\n" +
                        " (g)-[:TYPE {cost:2}]->(h),\n" +
                        " (h)-[:TYPE {cost:2}]->(i),\n" +
                        " (i)-[:TYPE {cost:2}]->(x)";

        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(api)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", 9999)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void test() throws Exception {

        ShortestPathDeltaStepping sssp = new ShortestPathDeltaStepping(graph, 3);

        head = getStartNode();
        System.out.println("startNode = " + head.getId());

        double[] map = sssp.compute(head.getId())
                .getShortestPaths();

        System.out.println("====================");
        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.printf("%2d -> %2d : %.1f%n", head.getId(), graph.toOriginalNodeId(i), map[i]);
        }
    }

    public Node getStartNode() throws Exception {
        try (Transaction tx = api.beginTx()) {
            final Node[] node = new Node[1];
            api.execute("MATCH (s:Node {start:true}) RETURN s").accept((Result.ResultVisitor<Exception>) row -> {
                node[0] = row.getNode("s");
                return false;
            });
            assertNotNull(node[0]);
            return node[0];
        }
    }
}
