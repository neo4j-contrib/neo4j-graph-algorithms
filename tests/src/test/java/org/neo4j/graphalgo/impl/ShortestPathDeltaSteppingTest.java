package org.neo4j.graphalgo.impl;

import algo.Pools;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**         5     5      5
 *      (A)---(B)---(C)----.
 *    5/ 2\2  2 \2  2 \2  2 \
 *  (S)---(G)---(H)---(I)---(X)--//->(S)
 *    3\  /3 3  /3 3  /3 3  /
 *      (D)---(E)---(F)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
public final class ShortestPathDeltaSteppingTest {

    private static GraphDatabaseAPI api;

    private static Graph graph;

    private static long head, tail;

    @BeforeClass
    public static void setup() {
        final String cypher =
                "CREATE (s:Node {name:'s'})\n" +
                        "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE (x:Node {name:'x'})\n" +
                        "CREATE" +
                        " (s)-[:TYPE {cost:5}]->(a),\n" +
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(x),\n" +

                        " (a)-[:TYPE {cost:2}]->(g),\n" +
                        " (b)-[:TYPE {cost:2}]->(h),\n" +
                        " (c)-[:TYPE {cost:2}]->(i),\n" +

                        " (s)-[:TYPE {cost:3}]->(d),\n" +
                        " (d)-[:TYPE {cost:3}]->(e),\n" +
                        " (e)-[:TYPE {cost:3}]->(f),\n" +
                        " (f)-[:TYPE {cost:3}]->(x),\n" +

                        " (d)-[:TYPE {cost:3}]->(g),\n" +
                        " (e)-[:TYPE {cost:3}]->(h),\n" +
                        " (f)-[:TYPE {cost:3}]->(i),\n" +

                        " (s)-[:TYPE {cost:2}]->(g),\n" +
                        " (g)-[:TYPE {cost:2}]->(h),\n" +
                        " (h)-[:TYPE {cost:2}]->(i),\n" +
                        " (i)-[:TYPE {cost:2}]->(x),\n" +

                        " (x)-[:TYPE {cost:2}]->(s)"; // create cycle

        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        head = getNode("s").getId();
        tail = getNode("x").getId();

        graph = new GraphLoader(api)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
//        Pools.DEFAULT.shutdownNow();
    }

    @Test
    public void testSequential() throws Exception {
        final ShortestPathDeltaStepping sssp = new ShortestPathDeltaStepping(graph, 3);

        final double[] sp = sssp.compute(head)
                .getShortestPaths();

        assertEquals(8, sp[graph.toMappedNodeId(tail)],0.1);
    }

    @Test
    public void testParallel() throws Exception {
        final ShortestPathDeltaStepping sssp = new ShortestPathDeltaStepping(graph, 3)
                .withExecutorService(Executors.newFixedThreadPool(3));

        final double[] sp = sssp.compute(head)
                .getShortestPaths();

        assertEquals(8, sp[graph.toMappedNodeId(tail)],0.1);
    }

    public static Node getNode(String name) {
        final Node[] node = new Node[1];
        api.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0];
    }
}
