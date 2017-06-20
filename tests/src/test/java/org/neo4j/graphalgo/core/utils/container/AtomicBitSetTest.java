package org.neo4j.graphalgo.core.utils.container;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author mknblch
 */
public class AtomicBitSetTest {

    private final AtomicBitSet set = new AtomicBitSet(Integer.MAX_VALUE);

    @Test
    public void testSmallNumber() throws Exception {
        assertFalse(set.get(0));
        set.set(0);
        assertTrue(set.get(0));
        set.unset(0);
        assertFalse(set.get(0));
    }

    @Test
    public void testBigNumber() throws Exception {
        assertFalse(set.get(Integer.MAX_VALUE));
        set.set(Integer.MAX_VALUE);
        assertTrue(set.get(Integer.MAX_VALUE));
        set.unset(Integer.MAX_VALUE);
        assertFalse(set.get(Integer.MAX_VALUE));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetInvalidValue() throws Exception {
        assertFalse(set.get(-1));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetInvalidValue() throws Exception {
        set.set(-1);
    }

    @Test
    public void test() throws Exception {
        final AtomicBitSet set = new AtomicBitSet(2);

        set.set(0);
        set.set(1);
        assertTrue(set.get(0));
        assertTrue(set.get(1));

        set.unset(1);
        assertTrue(set.get(0));
        assertFalse(set.get(1));

        set.set(1);
        set.unset(0);
        assertFalse(set.get(0));
        assertTrue(set.get(1));

        set.unset(0);
        set.unset(1);
        assertFalse(set.get(0));
        assertFalse(set.get(1));

    }
}
