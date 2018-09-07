package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class PagedLongLongBitSetTest {


    public static final int MAX = 65535;

    @Test
    public void newBitSet() throws Exception {

        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(65535, AllocationTracker.EMPTY);

        for (int i = 0; i < MAX; i++) {
            for (int j = 0; j < 10; j++) {
                bitSet.put(i, j, j % 2 == 0);
            }
        }

        boolean b = false;
        for (int i = 0; i < MAX; i++) {
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
    public void getOrDefault() throws Exception {

        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(65535, AllocationTracker.EMPTY);

        for (int i = 0; i < MAX; i++) {
            for (int j = 0; j < 10; j++) {
                assertTrue(bitSet.getOrDefault(i, j, true));
            }
        }
    }

    @Test
    public void flip() throws Exception {

        final PagedLongLongBitSet bitSet = PagedLongLongBitSet.newBitSet(65535, AllocationTracker.EMPTY);

        for (int i = 0; i < MAX; i++) {
            for (int j = 0; j < 10; j++) {
                bitSet.put(i, j, j % 2 == 0);
                bitSet.flip(i, j);
            }
        }

        boolean b = true;
        for (int i = 0; i < MAX; i++) {
            for (int j = 0; j < 10; j++) {
                final boolean c = bitSet.getOrDefault(i, j, false);
                if (c == b) {
                    fail(i + " " + j);
                }
                b = c;
            }
        }
    }

}