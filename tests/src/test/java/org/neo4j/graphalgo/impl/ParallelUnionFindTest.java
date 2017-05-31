package org.neo4j.graphalgo.impl;

import algo.Pools;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.leightweight.LightGraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * @author mknblch
 */
public class ParallelUnionFindTest {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;
    private static Graph graph;

    private static int mul = 1000;

    @BeforeClass
    public static void setup() {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph(10 * mul, 11 * mul, 12 * mul, 13 * mul, 14 * mul, 15 * mul, 16 * mul, 17 * mul);
        }

        db.execute("MATCH (n) RETURN n");

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(LightGraphFactory.class);

        System.out.println("graph.nodeCount() = " + graph.nodeCount());

    }

    @AfterClass
    public static void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }


    private static void createTestGraph(int... setSizes) {
        try (Transaction tx = db.beginTx()) {
            for (int setSize : setSizes) {
                createLine(setSize);
            }
            tx.success();
        }
    }

    private static void createLine(int setSize) {
        Node temp = db.createNode();
        for (int i = 1; i < setSize; i++) {
            Node t = db.createNode();
            temp.createRelationshipTo(t, RELATIONSHIP_TYPE);
            temp = t;
        }
    }

    @Test
    public void testSimple() throws Exception {
        graph.forEachRelationship(10000, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> true);
    }

    @Test
    public void test() throws Exception {

        System.out.println(graph.nodeCount());

        final DisjointSetStruct struct = new ParallelUnionFind(graph, Pools.DEFAULT, 10000)
                .compute()
                .getStruct();


        System.out.println(struct.getSetCount());

    }

}
