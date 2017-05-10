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
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
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
                .withRelationshipWeightsFromProperty("cost", 10)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void test() throws Exception {

        ShortestPathDeltaStepping sssp = new ShortestPathDeltaStepping(graph, 5);

        Node startNode = getStartNode();

        System.out.println("startNode = " + startNode.getId());

        LongDoubleMap map = sssp.compute(startNode.getId())
                .getShortestPaths();

        for (LongDoubleCursor c : map) {
            System.out.println(c.key + " -> " + c.index);
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
