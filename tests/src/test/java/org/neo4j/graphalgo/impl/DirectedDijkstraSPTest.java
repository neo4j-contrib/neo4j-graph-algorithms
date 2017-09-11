package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.procedures.IntProcedure;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.CypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

/**
 *     2    2   2
 *   ,->(b)->(c)->(f)
 *  |  1    1    1 |
 * (a)<-(d)<-(e)<-´
 *
 * @author mknblch
 */
public class DirectedDijkstraSPTest {

    public static final String cypher =
            "CREATE (a:Node {name:\"a\"})\n" +
            "CREATE (b:Node {name:\"b\"})\n" +
            "CREATE (c:Node {name:\"c\"})\n" +
            "CREATE (d:Node {name:\"d\"})\n" +
            "CREATE (e:Node {name:\"e\"})\n" +
            "CREATE (f:Node {name:\"f\"})\n" +
            "CREATE\n" +
            "  (a)-[:REL {cost:2}]->(b),\n" +
            "  (b)-[:REL {cost:2}]->(c),\n" +
            "  (c)-[:REL {cost:2}]->(f),\n" +
            "  (f)-[:REL {cost:1}]->(e),\n" +
            "  (e)-[:REL {cost:1}]->(d),\n" +
            "  (d)-[:REL {cost:1}]->(a)\n";


    private static final CypherTest cypherGraph = new CypherTest(cypher)
            .withLoader(loader -> {
                loader.withoutNodeProperties()
                        .withoutRelationshipWeights()
                        .withoutNodeWeights()
                        .withoutExecutorService();
            });

    private static GraphDatabaseAPI api;
    private static Graph graph;

    @BeforeClass
    public static void setup() {

        api = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();

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


    @Test
    public void testOutgoing() throws Exception {

//        new ShortestPathDijkstra(cypherGraph.build())
//                .compute(cypherGraph.findNode("Node", "name", "a").getId(), cypherGraph.findNode("Node", "name", "b").getId())
//                .getFinalPath()
//                .forEach((IntProcedure) System.out::println);

        new ShortestPathDijkstra(graph)
                .compute(0, 5)
                .getFinalPath()
                .forEach((IntProcedure) System.out::println);


    }
}
