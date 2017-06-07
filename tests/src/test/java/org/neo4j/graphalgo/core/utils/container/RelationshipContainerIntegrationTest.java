package org.neo4j.graphalgo.core.utils.container;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.Neo4JTestCase;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.sources.LazyIdMapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationshipContainerIntegrationTest extends Neo4JTestCase {

    private static RelationshipContainer container;

    private static int a, b, c;

    @Mock
    private RelationshipConsumer consumer;

    @BeforeClass
    public static void buildGraph() {

        a = newNode();
        b = newNode();
        c = newNode();

        newRelation(a, b);
        newRelation(a, c);
        newRelation(b, c);

        final LazyIdMapper idMapper = new LazyIdMapper(3);

        container = RelationshipContainer.importer((GraphDatabaseAPI) db)
                .withIdMapping(idMapper)
                .withDirection(Direction.OUTGOING)
                .build();

        a = idMapper.toMappedNodeId(a);
        b = idMapper.toMappedNodeId(b);
        c = idMapper.toMappedNodeId(c);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db!=null) db.shutdown();
    }

    @Test
    public void testV0ForEach() throws Exception {
        container.forEach(a, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(b), eq(-1L));
        verify(consumer, times(1)).accept(eq(a), eq(c), eq(-1L));
    }

    @Test
    public void testV1ForEach() throws Exception {
        container.forEach(b, consumer);
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(c), eq(-1L));
    }

    @Test
    public void testVXForEach() throws Exception {
        container.forEach(42, consumer);
        verify(consumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0Iterator() throws Exception {
        container.iterator(a).forEachRemaining(consume(consumer));
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(a), eq(b), eq(-1L));
        verify(consumer, times(1)).accept(eq(a), eq(c), eq(-1L));
    }

    @Test
    public void testV1Iterator() throws Exception {
        container.iterator(b).forEachRemaining(consume(consumer));
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(b), eq(c), eq(-1L));
    }

    @Test
    public void testVXIterator() throws Exception {
        container.iterator(42).forEachRemaining(consume(consumer));
        verify(consumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    private static Consumer<RelationshipCursor> consume(RelationshipConsumer consumer) {
        return cursor -> consumer.accept(cursor.sourceNodeId, cursor.targetNodeId, -1L);
    }
}
