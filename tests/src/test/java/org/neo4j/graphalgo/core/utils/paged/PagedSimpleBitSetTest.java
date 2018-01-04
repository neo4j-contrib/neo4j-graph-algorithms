package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class PagedSimpleBitSetTest {

    private final PagedSimpleBitSet set = PagedSimpleBitSet.newBitSet(Integer.MAX_VALUE + 100L, AllocationTracker.EMPTY);

    @Test
    public void testLowValues() throws Exception {
        assertFalse(set.contains(123));
        set.put(123);
        assertTrue(set.contains(123));
        set.clear();
        assertFalse(set.contains(123));
    }

    @Test
    public void testHighValues() throws Exception {
        assertFalse(set.contains(Integer.MAX_VALUE + 42L));
        set.put(Integer.MAX_VALUE + 42L);
        assertTrue(set.contains(Integer.MAX_VALUE + 42L));
        set.clear();
        assertHighEmpty();
    }

    private void assertHighEmpty() {
        for (long i = 0; i < 100; i++) {
            assertFalse(set.contains(Integer.MAX_VALUE + i));
        }
    }

}