package org.neo4j.graphalgo.impl.msbfs;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class BiMultiBitSet32Test extends RandomizedTest {

    @Test
    public void shouldSetAuxiliaryBit() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(1);
        for (int i = 0; i < 32; i++) {
            bitSet.setAuxBit(0, i);
            long expected = i == 31 ? 0xFFFFFFFF00000000L : (((1L << i + 1) - 1) << 32);
            assertEquals("" + i, expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldBuildSetUnionOnDefaultBit() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(1);
        int expected = 0;
        for (int i = 0; i < 3; i++) {
            int x = randomInt(Integer.MAX_VALUE);
            bitSet.union(0, x);
            expected |= x;
            assertEquals((long) expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldBuildSetUnionOnDefaultBitWhileKeepingTheAuxiliaryBit() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(1);
        int auxBit = between(1, 31);
        long auxValue = (1L << (32 + auxBit));
        bitSet.setAuxBit(0, auxBit);
        int expected = 0;
        for (int i = 0; i < 3; i++) {
            int x = randomInt(Integer.MAX_VALUE);
            bitSet.union(0, x);
            expected |= x;
            assertEquals(auxValue | ((long) expected), bitSet.get(0));
        }
    }

    @Test
    public void shouldTreatLowerBitsAsUnsigned() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(1);
        bitSet.union(0, Integer.MIN_VALUE);
        long expected = ((long) Integer.MAX_VALUE) + 1;
        assertEquals(expected, bitSet.get(0));
    }

    @Test
    public void shouldIterateToNextSetBit() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        int node = between(1, 9);
        bitSet.union(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldReturnStartNodeIfSet() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        int node = between(1, 9);
        bitSet.union(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(node));
    }

    @Test
    public void shouldReturnMinusOneIfNoMoreSetBits() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        int node = between(1, 5);
        bitSet.union(node, 42);
        assertEquals(-1, bitSet.nextSetNodeId(node + 1));
    }

    @Test
    public void shouldReturnMinusOneIfEmptyButCheckedFromMiddle() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        assertEquals(-1, bitSet.nextSetNodeId(between(1, 8)));
    }

    @Test
    public void shouldReturnMinusTwoIfEmpty() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        assertEquals(-2, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldOnlyIterateOverSetDefaultBits() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(10);
        bitSet.setAuxBit(between(1, 4), between(1, 31));
        int nextDefBit = between(5, 9);
        bitSet.union(nextDefBit, 42);
        assertEquals(nextDefBit, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldCalculateUnionDifference() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(1);
        // simulate 4 BFS, all 4 want to visit node 0
        bitSet.union(0, 15); // 15 = 1111
        // BFS 1 and 2 already have
        bitSet.setAuxBit(0, 1);
        bitSet.setAuxBit(0, 2);
        // check initial state, 6 = 0110
        assertEquals(15L | (6L << 32), bitSet.get(0));

        int diff = bitSet.unionDifference(0);

        // 9 == 1001
        assertEquals(9, diff);
        assertEquals(9L | (15L << 32), bitSet.get(0));
    }

    @Test
    public void shouldCopyIntoSimpleBS32() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(2);
        bitSet.union(0, 42);
        bitSet.union(1, 1337);

        MultiBitSet32 bitSet2 = new MultiBitSet32(2);
        bitSet.copyInto(bitSet2);

        assertEquals(0L, bitSet.get(0));
        assertEquals(0L, bitSet.get(1));
        assertEquals(42, bitSet2.get(0));
        assertEquals(1337, bitSet2.get(1));
    }

    @Test
    public void shouldKeepAuxiliaryBitsWhileCopying() {
        BiMultiBitSet32 bitSet = new BiMultiBitSet32(2);
        bitSet.setAuxBit(0, 4);
        bitSet.setAuxBit(1, 2);
        bitSet.union(0, 42);
        bitSet.union(1, 1337);

        MultiBitSet32 bitSet2 = new MultiBitSet32(2);
        bitSet.copyInto(bitSet2);

        assertEquals(1L << 36, bitSet.get(0));
        assertEquals(1L << 34, bitSet.get(1));
        assertEquals(42, bitSet2.get(0));
        assertEquals(1337, bitSet2.get(1));
    }
}
