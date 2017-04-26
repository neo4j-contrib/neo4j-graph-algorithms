package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.graphbuilder.RingBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;


/**
 * @author mknblch
 */
public class BetweennessCentralityTest {

    public static final String TYPE = "TYPE";
    private static GraphDatabaseAPI db;
    private static Graph graph;
    private static DefaultBuilder builder;

    @BeforeClass
    public static void setupGraph() {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        builder = GraphBuilder.create(db)
                .setRelationship(TYPE);

        final RelationshipType type = RelationshipType.withName(TYPE);

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         *
         * TODO should have the highest centrality ;)
         */
        final Node center = builder.newDefaultBuilder()
                .createNode();

        builder.newRingBuilder()
                .createRing(5)
                .forEachInTx(node -> {
                    node.createRelationshipTo(center, type);
                })
                .newRingBuilder()
                .createRing(5)
                .forEachInTx(node -> {
                    center.createRelationshipTo(node, type);
                });

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        db.shutdown();
    }

    @Test
    public void test() throws Exception {

        // TODO build appropriate testcase

        new BetweennessCentrality(graph)
                .compute()
                .forEach((originalNodeId, value) -> {
                    System.out.println("centrality:" + originalNodeId + ": " + value);
                    return true;
                });
    }
}
