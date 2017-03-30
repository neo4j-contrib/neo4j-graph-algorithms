package org.neo4j.graphalgo.core.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mknobloch
 */
public class DisjointSetStructTest {

    private DisjointSetStruct struct;

    @Before
    public void setup() {
        struct = new DisjointSetStruct(3);
    }

    @Test
    public void test() throws Exception {

        assertEquals(0, struct.find(0));
        assertEquals(1, struct.find(1));
        assertEquals(2, struct.find(2));

        assertFalse(struct.isConnected(0, 1));
        struct.union(1, 0);
        assertTrue(struct.isConnected(0, 1));

        assertFalse(struct.isConnected(1, 2));
        struct.union(2, 1);
        assertTrue(struct.isConnected(1, 2));

    }
}
