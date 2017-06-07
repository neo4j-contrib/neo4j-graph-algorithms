package org.neo4j.graphalgo.impl;

import algo.Pools;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
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

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
@Ignore
public class ParallelUnionFindTest {

    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private static GraphDatabaseAPI db;
    private static Graph graph;

    private static int mul = 10;

    @BeforeClass
    public static void setup() {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph(mul, mul, mul, mul, mul, mul, mul, mul);
        }

        db.execute("MATCH (n) RETURN n");

        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);


    }

    @AfterClass
    public static void shutdown() {
        db.shutdown();
//        Pools.DEFAULT.shutdownNow();
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
    public void testBigSet() throws Exception {

        System.out.println("graph.nodeCount() = " + graph.nodeCount());


        final DisjointSetStruct struct = new ParallelUnionFindQueue(graph, Pools.DEFAULT, mul)
                .compute()
                .getStruct();
        assertEquals(8, struct.getSetCount());

//        System.out.println(struct);
//        System.out.println("struct.getSetCount() = " + struct.getSetCount());
    }

}
