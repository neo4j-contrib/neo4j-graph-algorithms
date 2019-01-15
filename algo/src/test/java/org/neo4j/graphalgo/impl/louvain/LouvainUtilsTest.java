package org.neo4j.graphalgo.impl.louvain;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LouvainUtilsTest {
    @Test
    public void differentNumbers() {
        int[] communities = {10, 3, 4, 7, 6, 7, 10};
        assertEquals(5, LouvainUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 3, 0}, communities);
    }

    @Test
    public void allTheSame() {
        int[] communities = {10, 10, 10, 10};
        assertEquals(1, LouvainUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 0, 0, 0}, communities);
    }

    @Test
    public void allDifferent() {
        int[] communities = {1, 2, 3, 4, 7, 5};
        assertEquals(6, LouvainUtils.normalize(communities));
        assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5}, communities);
    }
}