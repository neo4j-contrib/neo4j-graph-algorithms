package org.neo4j.graphalgo.core.heavyweight;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknobloch
 */
@RunWith(MockitoJUnitRunner.class)
public class HeavyGraphFactoryTest {

    private static GraphDatabaseService db;

    private static long id1;
    private static long id2;
    private static long id3;

    @Mock
    private RelationshipConsumer relationConsumer;

    @Mock
    private WeightedRelationshipConsumer weightedRelationConsumer;

    @BeforeClass
    public static void setup() {

        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();

        try (final Transaction transaction = db.beginTx()) {
            final Node node1 = db.createNode(Label.label("Node1"));
            final Node node2 = db.createNode(Label.label("Node2"));
            final Node node3 = db.createNode(Label.label("Node3"));
            final Relationship rel1 = node1.createRelationshipTo(node2, RelationshipType.withName("REL1"));
            final Relationship rel2 = node1.createRelationshipTo(node3, RelationshipType.withName("REL2"));
            final Relationship rel3 = node2.createRelationshipTo(node3, RelationshipType.withName("REL3"));
            rel1.setProperty("prop1", 1);
            rel2.setProperty("prop2", 2);
            rel3.setProperty("prop3", 3);
            transaction.success();

            id1 = node1.getId();
            id2 = node2.getId();
            id3 = node3.getId();
        }

    }

    @Test
    public void testAnyLabel() throws Exception {

        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);

        assertEquals(3, graph.nodeCount());
    }

    @Test
    public void testWithLabel() throws Exception {

        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withLabel("Node1")
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);

        assertEquals(1, graph.nodeCount());
    }

    @Test
    public void testAnyRelation() throws Exception {
        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HeavyGraphFactory.class);

        graph.forEachRelationship(graph.toMappedNodeId(id1), Direction.OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(graph.toMappedNodeId(id1)), eq(graph.toMappedNodeId(id3)), anyLong());
        verify(relationConsumer, times(1)).accept(eq(graph.toMappedNodeId(id1)), eq(graph.toMappedNodeId(id2)), anyLong());
        Mockito.reset(relationConsumer);

        graph.forEachRelationship(graph.toMappedNodeId(id2), Direction.OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(graph.toMappedNodeId(id2)), eq(graph.toMappedNodeId(id3)), anyLong());
        Mockito.reset(relationConsumer);
    }

    @Test
    public void testWithRelation() throws Exception {
        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withRelationshipType("REL1")
                .load(HeavyGraphFactory.class);

        graph.forEachRelationship(graph.toMappedNodeId(id1), Direction.OUTGOING, relationConsumer);
        verify(relationConsumer, times(1)).accept(eq(graph.toMappedNodeId(id1)), eq(graph.toMappedNodeId(id2)), anyLong());
        verify(relationConsumer, never()).accept(eq(graph.toMappedNodeId(id1)), eq(graph.toMappedNodeId(id3)), anyLong());
        Mockito.reset(relationConsumer);

        graph.forEachRelationship(graph.toMappedNodeId(id2), Direction.OUTGOING, relationConsumer);
        verify(relationConsumer, never()).accept(anyInt(), anyInt(), anyLong());
        Mockito.reset(relationConsumer);
    }

    @Test
    public void testWithProperty() throws Exception {

        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("prop1", 0.0)
                .load(HeavyGraphFactory.class);

        graph.forEachRelationship(graph.toMappedNodeId(id1), Direction.OUTGOING, weightedRelationConsumer);
        verify(weightedRelationConsumer, times(1))
                .accept(eq(graph.toMappedNodeId(id1)), eq(graph.toMappedNodeId(id2)), anyLong(), eq(1.0));
    }

}
