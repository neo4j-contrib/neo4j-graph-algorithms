package org.neo4j.graphalgo.impl;

import algo.Pools;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.procedures.IntObjectProcedure;
import com.carrotsearch.hppc.procedures.ObjectProcedure;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class MultistepSCCTest {


    private static GraphDatabaseAPI api;

    private static Graph graph;

    @BeforeClass
    public static void setup() {
        final String cypher =
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
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(a),\n" +

                        " (d)-[:TYPE {cost:2}]->(e),\n" +
                        " (e)-[:TYPE {cost:2}]->(f),\n" +
                        " (f)-[:TYPE {cost:2}]->(d),\n" +

                        " (a)-[:TYPE {cost:2}]->(d),\n" +

                        " (g)-[:TYPE {cost:3}]->(h),\n" +
                        " (h)-[:TYPE {cost:3}]->(i),\n" +
                        " (i)-[:TYPE {cost:3}]->(g)";

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
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
//        Pools.DEFAULT.shutdownNow();
    }

    public static int getMappedNodeId(String name) {
        final Node[] node = new Node[1];
        api.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    private IntSet allNodes() {
        final IntScatterSet nodes = new IntScatterSet();
        for (int i = 0; i < graph.nodeCount(); i++) {
            nodes.add(i);
        }
        return nodes;
    }

    @Test
    public void testSequential() throws Exception {

        final IntObjectMap<IntSet> connectedComponents =
                new MultistepSCC(graph, Pools.DEFAULT, 1, 0)
                        .compute()
                        .getConnectedComponents();

        assertCC(connectedComponents);
    }

    @Test
    public void testParallel() throws Exception {

        final IntObjectMap<IntSet> connectedComponents =
                new MultistepSCC(graph, Pools.DEFAULT, 4, 0)
                        .compute()
                        .getConnectedComponents();

        assertCC(connectedComponents);
    }

    @Test
    public void testHighCut() throws Exception {

        final IntObjectMap<IntSet> connectedComponents =
                new MultistepSCC(graph, Pools.DEFAULT, 4, 100_000)
                        .compute()
                        .getConnectedComponents();

        assertCC(connectedComponents);
    }

    private void assertCC(IntObjectMap<IntSet> connectedComponents) {
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("a"),
                getMappedNodeId("b"),
                getMappedNodeId("c"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("d"),
                getMappedNodeId("e"),
                getMappedNodeId("f"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("g"),
                getMappedNodeId("h"),
                getMappedNodeId("i"));
    }

    private static void assertBelongSameSet(IntObjectMap<IntSet> result, int... expected) {
        final int needle = expected[0];
        result.forEach((Consumer<? super IntObjectCursor<IntSet>>) cursor -> {
            if (cursor.value.contains(needle)) {
                assertEquals("Set size differs", expected.length, cursor.value.size());
                for (int i : expected) {
                    assertTrue("Set " + cursor.value + " did not contain " + i, cursor.value.contains(i));
                }
            }
        });
    }
}
