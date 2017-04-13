package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntMap;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mknblch
 */
public class DisjointSetStructTest {

    private DisjointSetStruct struct = new DisjointSetStruct(7);

    @Test
    public void test() throws Exception {

        // {0}{1}{2}{3}{4}{5}{6}
        assertFalse(struct.connected(0, 1));

        struct.union(0, 1);
        assertTrue(struct.connected(0, 1));
        // {0,1}{2}{3}{4}{5}{6}
        assertFalse(struct.connected(2, 3));

        struct.union(2, 3);
        assertTrue(struct.connected(2, 3));
        // {0,1}{2,3}{4}{5}{6}
        assertFalse(struct.connected(0, 2));
        assertFalse(struct.connected(0, 3));
        assertFalse(struct.connected(1, 2));
        assertFalse(struct.connected(1, 3));

        struct.union(3, 0);
        assertTrue(struct.connected(0, 2));
        assertTrue(struct.connected(0, 3));
        assertTrue(struct.connected(1, 2));
        assertTrue(struct.connected(1, 3));
        // {0,1,2,3}{4}{5}{6}
        assertFalse(struct.connected(4, 5));

        struct.union(4, 5);
        assertTrue(struct.connected(4, 5));
        // {0,1,2,3}{4,5}{6}
        assertFalse(struct.connected(0, 4));

        struct.union(0, 4);
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
        // {0,1,2,3,4,5}{6}

        final IntIntMap setSize = struct.getSetSize();
        System.out.println(setSize);
    }
}
