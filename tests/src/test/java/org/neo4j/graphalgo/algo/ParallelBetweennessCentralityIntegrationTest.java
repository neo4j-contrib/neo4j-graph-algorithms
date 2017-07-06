package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.core.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.BetweennessCentrality;
import org.neo4j.graphalgo.impl.BetweennessCentralitySuccessorBrandes;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class ParallelBetweennessCentralityIntegrationTest {

    public static final String TYPE = "TYPE";

    private static GraphDatabaseAPI db;
    private static Graph graph;
    private static DefaultBuilder builder;
    private static long centerNodeId;

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        builder = GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship(TYPE);

        final RelationshipType type = RelationshipType.withName(TYPE);

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        final Node center = builder.newDefaultBuilder()
                .setLabel("Node")
                .createNode();

        centerNodeId = center.getId();

        builder.newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    node.createRelationshipTo(center, type);
                })
                .newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    center.createRelationshipTo(node, type);
                });

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(HeavyGraphFactory.class);

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);
    }

    @Before
    public void setupMocks() {
        when(consumer.consume(anyLong(), anyDouble()))
                .thenReturn(true);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db!=null) db.shutdown();
        graph = null;
    }

    @Test
    public void testDirect() throws Exception {
        new BetweennessCentralitySuccessorBrandes(graph, 100_000,
                Pools.DEFAULT)
                .compute()
                .forEach(consumer);
        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

}
