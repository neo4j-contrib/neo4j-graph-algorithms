package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class PagedLongLongBitSetTest {


    public static final int CAPACITY = 65535;

    @Test
    public void newBitSet() throws Exception {
        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(CAPACITY, AllocationTracker.EMPTY);
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                bitSet.put(i, j, j % 2 == 0);
            }
        }
        boolean b = false;
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                final boolean c = bitSet.getOrDefault(i, j, false);
                // check for alternating boolean
                if (c == b) {
                    fail(i + " " + j);
                }
                b = c;
            }
        }
    }

    @Test
    public void getOrDefault() throws Exception {
        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(CAPACITY, AllocationTracker.EMPTY);
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                assertTrue(bitSet.getOrDefault(i, j, true));
            }
        }
    }

    @Test
    public void flip() throws Exception {
        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(CAPACITY, AllocationTracker.EMPTY);
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                bitSet.put(i, j, j % 2 == 0);
                bitSet.flip(i, j);
            }
        }
        boolean b = true; // flipped, starts with true
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                final boolean c = bitSet.getOrDefault(i, j, false);
                if (c == b) {
                    fail(i + " " + j);
                }
                b = c;
            }
        }
    }

    @Test
    public void clear() throws Exception {
        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(CAPACITY, AllocationTracker.EMPTY);
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                bitSet.put(i, j, true);
            }
        }
        bitSet.clear();
        for (int i = 0; i < CAPACITY; i++) {
            for (int j = 0; j < 10; j++) {
                assertFalse(bitSet.getOrDefault(i, j, false));
            }
        }
    }

}