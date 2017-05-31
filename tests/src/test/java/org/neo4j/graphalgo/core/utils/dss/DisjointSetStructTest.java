package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author mknblch
 */
public class DisjointSetStructTest {

    private DisjointSetStruct struct = new DisjointSetStruct(7);

    @Before
    public void setup() {
        struct.reset();
    }

    @Test
    public void testSetUnion() throws Exception {

        // {0}{1}{2}{3}{4}{5}{6}
        assertFalse(struct.connected(0, 1));
        assertEquals(7, struct.getSetSize().size());

        struct.union(0, 1);
        // {0,1}{2}{3}{4}{5}{6}
        assertTrue(struct.connected(0, 1));
        assertFalse(struct.connected(2, 3));
        assertEquals(6, struct.getSetSize().size());

        struct.union(2, 3);
        // {0,1}{2,3}{4}{5}{6}
        assertTrue(struct.connected(2, 3));
        assertFalse(struct.connected(0, 2));
        assertFalse(struct.connected(0, 3));
        assertFalse(struct.connected(1, 2));
        assertFalse(struct.connected(1, 3));
        assertEquals(5, struct.getSetSize().size());

        struct.union(3, 0);
        // {0,1,2,3}{4}{5}{6}
        assertTrue(struct.connected(0, 2));
        assertTrue(struct.connected(0, 3));
        assertTrue(struct.connected(1, 2));
        assertTrue(struct.connected(1, 3));
        assertFalse(struct.connected(4, 5));
        assertEquals(4, struct.getSetSize().size());

        struct.union(4, 5);
        // {0,1,2,3}{4,5}{6}
        assertTrue(struct.connected(4, 5));
        assertFalse(struct.connected(0, 4));
        assertEquals(3, struct.getSetSize().size());

        struct.union(0, 4);
        // {0,1,2,3,4,5}{6}
        assertTrue(struct.connected(0, 4));
        assertTrue(struct.connected(0, 5));
        assertTrue(struct.connected(1, 4));
        assertTrue(struct.connected(1, 5));
        assertTrue(struct.connected(2, 4));
        assertTrue(struct.connected(2, 5));
        assertTrue(struct.connected(3, 4));
        assertTrue(struct.connected(3, 5));
        assertTrue(struct.connected(4, 5));
        assertFalse(struct.connected(0, 6));
        assertFalse(struct.connected(1, 6));
        assertFalse(struct.connected(2, 6));
        assertFalse(struct.connected(3, 6));
        assertFalse(struct.connected(4, 6));
        assertFalse(struct.connected(5, 6));

        final IntIntMap setSize = struct.getSetSize();
        System.out.println(setSize);
        assertEquals(2, setSize.size());
        for (IntIntCursor cursor : setSize) {
            assertTrue(cursor.value == 6 || cursor.value == 1);
        }
    }

    @Test
    public void testDefault() throws Exception {
        DisjointSetStruct.Consumer consumer = mock(DisjointSetStruct.Consumer.class);
        when(consumer.consume(anyInt(), anyInt())).thenReturn(true);
        struct.forEach(consumer);
        verify(consumer, times(7)).consume(anyInt(), anyInt());
        verify(consumer, times(1)).consume(eq(0), eq(0));
        verify(consumer, times(1)).consume(eq(1), eq(1));
        verify(consumer, times(1)).consume(eq(2), eq(2));
        verify(consumer, times(1)).consume(eq(3), eq(3));
        verify(consumer, times(1)).consume(eq(4), eq(4));
        verify(consumer, times(1)).consume(eq(5), eq(5));
        verify(consumer, times(1)).consume(eq(6), eq(6));
    }

    @Test
    public void testMergeDSS() throws Exception {
        final DisjointSetStruct a = create(10, set(0, 1, 2, 3), set(4, 5, 6), set(7, 8), set(9));
        final DisjointSetStruct b = create(10, set(0, 5), set(7, 9));
        assertEquals(4, a.getSetCount());
        a.merge(b);
        assertEquals(2, a.getSetCount());
        System.out.println(a);
    }

    public static int[] set(int... elements) {
        return elements;
    }

    private static DisjointSetStruct create(int size, int[]... sets) {
        final DisjointSetStruct dss = new DisjointSetStruct(size).reset();
        for (int[] set : sets) {
            if (set.length < 1) {
                throw new IllegalArgumentException("Sets must contain at least one element");
            }
            for (int i = 1; i < set.length; i++) {
                dss.union(set[i], set[0]);
            }
        }
        return dss;
    }

    @Test
    public void test() throws Exception {
        System.out.println(new DisjointSetStruct(5).reset());
    }
}
