package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.yens.Dijkstra;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphalgo.impl.yens.YensKShortestPaths;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 *
 * https://en.wikipedia.org/wiki/Yen%27s_algorithm
 *
 * @author mknblch
 */
public class YensWikiTest {

    private static final int[][] EXPECTED = new int[][]{
        new int[]{0, 2, 3, 5},
        new int[]{0, 2, 4, 5},
        new int[]{0, 2, 3, 4, 5},
    };

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE" +
                        " (c)-[:TYPE {cost:3.0}]->(d),\n" +
                        " (c)-[:TYPE {cost:2.0}]->(e),\n" +
                        " (e)-[:TYPE {cost:1.0}]->(d),\n" +
                        " (e)-[:TYPE {cost:2.0}]->(f),\n" +
                        " (e)-[:TYPE {cost:3.0}]->(g),\n" +
                        " (d)-[:TYPE {cost:4.0}]->(f),\n" +

                        " (f)-[:TYPE {cost:2.0}]->(g),\n" +
                        " (f)-[:TYPE {cost:1.0}]->(h),\n" +
                        " (g)-[:TYPE {cost:2.0}]->(h)";

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);

    }
    public static Node getNode(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0];
    }


    @Test
    public void test() throws Exception {

        final YensKShortestPaths yens = new YensKShortestPaths(graph).compute(
                getNode("c").getId(),
                getNode("h").getId(),
                Direction.BOTH,
                3);

        final List<WeightedPath> paths = yens.getPaths();

        for (int i = 0; i < paths.size(); i++) {
            final WeightedPath weightedPath = paths.get(i);
            assertArrayEquals(EXPECTED[i], weightedPath.toArray());
        }

    }

    @Test
    public void testDijkstra() throws Exception {

        final Dijkstra dijkstra = new Dijkstra(graph);

        final Optional<WeightedPath> path = dijkstra
                .withDirection(Direction.BOTH)
                .compute(graph.toMappedNodeId(getNode("c").getId()),
                        graph.toMappedNodeId(getNode("h").getId()));

        if (!path.isPresent()) {
            fail();
        }

        assertArrayEquals(new int[]{0, 2, 3 ,5}, path.get().toArray());
    }

}
