package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertArrayEquals;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  | X |     | X |
 * (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
public class LouvainClusteringTest {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        
                        "CREATE" +
                        
                        " (a)-[:TYPE  {w:0.5}]->(b),\n" +
                        " (a)-[:TYPE  {w:0.5}]->(c),\n" +
                        " (a)-[:TYPE  {w:0.5}]->(d),\n" +
                        " (c)-[:TYPE  {w:0.5}]->(d),\n" +
                        " (b)-[:TYPE  {w:0.5}]->(c),\n" +
                        " (b)-[:TYPE  {w:0.5}]->(d),\n" +
                        
                        " (e)-[:TYPE  {w:0.5}]->(f),\n" +
                        " (e)-[:TYPE  {w:0.5}]->(g),\n" +
                        " (e)-[:TYPE  {w:0.5}]->(h),\n" +
                        " (f)-[:TYPE  {w:0.5}]->(h),\n" +
                        " (f)-[:TYPE  {w:0.5}]->(g),\n" +
                        
                        " (b)-[:TYPE {w:0.5}]->(e)";


        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("w", 1.0)
                .load(HeavyGraphFactory.class);

    }

    @Test
    public void test() throws Exception {

        final int[] communities = new Louvain(graph, Pools.DEFAULT, 1, 10_000)
                .compute()
                .getCommunities();

        for (int i = 0; i < communities.length; i++) {
            System.out.println(i + " : " + communities[i]);
        }
    }
}
