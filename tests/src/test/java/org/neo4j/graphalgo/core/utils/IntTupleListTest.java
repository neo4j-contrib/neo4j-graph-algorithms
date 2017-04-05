package org.neo4j.graphalgo.core.utils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class IntTupleListTest {

    private final IntTupleList tuples = new IntTupleList(3, 3);

    @Mock
    private TestConsumer testConsumer;

    @Before
    public void setup() throws Exception {
        tuples.add(1, 1, -1);
        tuples.add(2, 2, -1);
        tuples.add(3, 3, -1);
    }

    @Test
    public void testAdd() throws Exception {
        IntTupleList tuples = new IntTupleList(2, 0);

        assertEquals(0, tuples.add(0, 0));
        assertEquals(1, tuples.add(0, 0));
        assertEquals(2, tuples.add(0, 0));
    }

    @Test
    public void testIterator() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.forEach(adapter);
        verify(testConsumer, times(3)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(3, tuples.size());
    }

    @Test
    public void testSliceSize01() throws Exception {
        assertEquals(1, tuples.slice(0, 1).size());
    }

    @Test
    public void testSliceSize13() throws Exception {
        assertEquals(2, tuples.slice(1, 3).size());
    }

    @Test
    public void testSliceSize23() throws Exception {
        assertEquals(1, tuples.slice(2, 3).size());
    }

    @Test
    public void testSliceSize12() throws Exception {
        assertEquals(1, tuples.slice(1, 2).size());
    }

    @Test
    public void testSlice01() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 1).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
    }

    @Test
    public void testSlice02() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 2).forEach(adapter);
        verify(testConsumer, times(2)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
    }

    @Test
    public void testSlice03() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 3).forEach(adapter);
        verify(testConsumer, times(3)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    @Test
    public void testSlice23() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(2, 3).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    @Test
    public void testSlice12() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(1, 2).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
    }

    @Test
    public void testSlice13() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(1, 3).forEach(adapter);
        verify(testConsumer, times(2)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    @Test
    public void testSlice11() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(1, 1).forEach(adapter);
        verify(testConsumer, never()).consume(anyInt(), anyInt(), anyInt());
    }

    @Test
    public void testSlice22() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(2, 2).forEach(adapter);
        verify(testConsumer, never()).consume(anyInt(), anyInt(), anyInt());
    }

    @Test
    public void testSlice33() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(3, 3).forEach(adapter);
        verify(testConsumer, never()).consume(anyInt(), anyInt(), anyInt());
    }

    @Test
    public void testSubSplice02_01() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 2).slice(0, 1).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
    }

    @Test
    public void testSubSplice03_01() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 3).slice(0, 1).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(1), eq(1), eq(-1));
    }

    @Test
    public void testSubSplice03_23() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(0, 3).slice(2, 3).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    @Test
    public void testSubSplice13_01() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(1, 3).slice(0, 1).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(2), eq(2), eq(-1));
    }

    @Test
    public void testSubSplice23_01() throws Exception {
        Consumer<int[]> adapter = adapter();
        tuples.slice(2, 3).slice(0, 1).forEach(adapter);
        verify(testConsumer, times(1)).consume(anyInt(), anyInt(), anyInt());
        verify(testConsumer, times(1)).consume(eq(3), eq(3), eq(-1));
    }

    private interface TestConsumer {
        void consume(int a, int b, int c);
    }

    private Consumer<int[]> adapter() {
        return array -> testConsumer.consume(array[0], array[1], array[2]);
    }
}
