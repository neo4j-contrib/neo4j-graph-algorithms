package org.neo4j.graphalgo.core.sources;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class BothRelationshipAdapterIntegrationTest extends Neo4JTestCase {

    @Mock
    private RelationshipConsumer consumer;

    private static int a, b, c;
    private static LazyIdMapper idMapper;
    private static BufferedWeightMap weightMap;
    private static BothRelationshipAdapter bothRelationshipAdapter;

    @BeforeClass
    public static void setupGraph() {
        a = newNode();
        b = newNode();
        c = newNode();
        newRelation(a, b, 1);
        newRelation(a, c, 2);
        newRelation(b, c, 3);

        idMapper = new LazyIdMapper(3);
        RelationshipContainer relationshipContainer = RelationshipContainer.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withDirection(Direction.BOTH)
                .withLabel(LABEL)
                .withRelationshipType(RELATION)
                .withWeightsFromProperty(WEIGHT_PROPERTY, 0.0)
                .build();
        bothRelationshipAdapter = new BothRelationshipAdapter(relationshipContainer);

        a = idMapper.toMappedNodeId(a);
        b = idMapper.toMappedNodeId(b);
        c = idMapper.toMappedNodeId(c);
    }

    @Before
    public void setupMocks() {
        when(consumer.accept(anyInt(), anyInt(), anyLong())).thenReturn(true);
    }

    @Test
    public void testFromA() throws Exception {
        bothRelationshipAdapter.forEachRelationship(a, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(b), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(c), anyLong());
    }

    @Test
    public void testFromB() throws Exception {
        bothRelationshipAdapter.forEachRelationship(b, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(a), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(c), anyLong());
    }

    @Test
    public void testFromC() throws Exception {
        bothRelationshipAdapter.forEachRelationship(c, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(c), eq(a), anyLong());
        verify(consumer, times(1)).accept(eq(c), eq(b), anyLong());
    }
}
