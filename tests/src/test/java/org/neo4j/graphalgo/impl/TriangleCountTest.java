package org.neo4j.graphalgo.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 *   (c)    (d)
 *   /  \
 * (a)--(b)
 *
 * @author mknblch
 */
public class TriangleCountTest {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    @BeforeClass
    public static void setup() throws Exception {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        String cypher = "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE " +
                "(a)-[:TYPE]->(b),\n" +
                "(b)-[:TYPE]->(c),\n" +
                "(c)-[:TYPE]->(a),\n" +
                "(c)-[:TYPE]->(d)";

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        };

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .load(HeavyGraphFactory.class);
    }

    @Test
    public void testSequential() throws Exception {
        final int[] triangles = new ParallelTriangleCount(graph, Pools.DEFAULT, 1)
                .compute()
                .getTriangles();

        for (int i = 0; i < triangles.length; i++) {
            System.out.println(i + " " + triangles[i]);
        }
    }

}
