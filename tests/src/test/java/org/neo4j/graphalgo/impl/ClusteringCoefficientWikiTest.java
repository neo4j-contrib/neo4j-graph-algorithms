package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 *   C(x) = (2 * triangles(x)) / (degree(x) * (degree(x) - 1))
 *   C(G) = 1/n * sum( C(x) )
 *
 *
 *   (c)           N |T |L |C
 *   /             --+--+--+----
 * (a)--(b)        a |1 |6 |0.33
 *   \  /          b |1 |2 |1.0
 *   (d)           c |0 |1 |0.0
 *                 d |1 |2 |1.0
 *
 * @author mknblch
 */
public class ClusteringCoefficientWikiTest {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    private static final double[] EXPECTED = {0.33, 1.0, 0.0, 1.0};

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(d)";

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void test() throws Exception {
        final TriangleCount algo =
                new TriangleCount(graph, Pools.DEFAULT, 4)
                        .compute();
        assertArrayEquals(EXPECTED, algo.getClusteringCoefficients(), 0.1);
        assertEquals(0.583, algo.getAverageClusteringCoefficient(), 0.01);
    }
}
