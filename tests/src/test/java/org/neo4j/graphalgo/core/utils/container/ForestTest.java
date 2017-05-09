package org.neo4j.graphalgo.core.utils.container;

import org.junit.Test;

import java.util.function.IntPredicate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
public class ForestTest {

    @Test
    public void testEmptyForest() throws Exception {

        assertEquals(0, new Forest(1).size(0));
    }

    @Test
    public void test() throws Exception {

        final IntPredicate consumer = mock(IntPredicate.class);
        when(consumer.test(anyInt())).thenReturn(true);

        new Forest(1)
                .append(0, 0)
                .append(0, 1)
                .append(0, 2)
                .forEach(0, consumer);

        verify(consumer, times(3)).test(anyInt());
        verify(consumer, times(1)).test(eq(0));
        verify(consumer, times(1)).test(eq(1));
        verify(consumer, times(1)).test(eq(2));
    }
}
