package org.neo4j.graphalgo.core.utils.container;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.container.RelationshipContainer;

import java.util.function.Consumer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationshipContainerTest {

    private static RelationshipContainer container;

    @Mock
    private RelationshipConsumer consumer;

    @BeforeClass
    public static void setup() {
        container = RelationshipContainer.builder(2)
                .aim(0, 2)
                    .add(1)
                    .add(2)
                .aim(1, 1)
                    .add(2)
                .build();
    }


    @Test
    public void testV0ForEach() throws Exception {
        container.forEach(0, consumer);
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(0), eq(1), eq(-1L));
        verify(consumer, times(1)).accept(eq(0), eq(2), eq(-1L));
    }

    @Test
    public void testV1ForEach() throws Exception {
        container.forEach(1, consumer);
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(1), eq(2), eq(-1L));
    }

    @Test
    public void testVXForEach() throws Exception {
        container.forEach(42, consumer);
        verify(consumer, never()).accept(anyInt(), anyInt(), anyLong());
    }

    @Test
    public void testV0Iterator() throws Exception {
        container.iterator(0).forEachRemaining(consume(consumer));
        verify(consumer, times(2)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(0), eq(1), eq(-1L));
        verify(consumer, times(1)).accept(eq(0), eq(2), eq(-1L));
    }

    @Test
    public void testV1Iterator() throws Exception {
        container.iterator(1).forEachRemaining(consume(consumer));
        verify(consumer, times(1)).accept(anyInt(), anyInt(), anyLong());
        verify(consumer, times(1)).accept(eq(1), eq(2), eq(-1L));
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
