package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  |  /       \  |   (z)
 *  (c)         (h)
 *
 * @author mknblch
 */
public class LouvainTest_398 {

    private static GraphDatabaseAPI db;
    private static HeavyGraph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (z:Node {name:'z'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (c:Node {name:'c'})\n" + // shuffled
                        "CREATE (h:Node {name:'h'})\n" +

                        "CREATE" +
                        
                        " (b)-[:TYPE]->(a),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (e)-[:TYPE]->(f),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(h),\n" +

                        " (b)-[:TYPE]->(e)";


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
                .withRelationshipWeightsFromProperty("w", 1.0)
                .load(HeavyGraphFactory.class);

    }

    private String getName(long nodeId) {
        String[] name = {""};
        try (Transaction tx = db.beginTx()) {
            db.execute(String.format("MATCH (n) WHERE id(n) = %d RETURN n", nodeId)).accept(row -> {
                name[0] = (String) row.getNode("n").getProperty("name");
                return true;
            });
        }
        return name[0];
    }

    @Test
    public void test() throws Exception {
        final Louvain louvain = new Louvain(graph, graph, graph, Pools.DEFAULT, 1)
                .compute(10);
        final int[] communities = louvain.getCommunityIds();
        for (int i = 0; i < communities.length; i++) {
            System.out.println(getName(i) + " : " + communities[i]);
        }
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        assertEquals(3, louvain.getCommunityCount());
    }

    @Test
    public void testParallel() throws Exception {
        final Louvain louvain = new Louvain(graph, graph, graph, Pools.DEFAULT, 8)
                .compute(10);
        final int[] communities = louvain.getCommunityIds();
        for (int i = 0; i < communities.length; i++) {
            System.out.println(getName(i) + " : " + communities[i]);
        }
        System.out.println("louvain.getRuns() = " + louvain.getIterations());
        assertEquals(3, louvain.getCommunityCount());
    }
}
