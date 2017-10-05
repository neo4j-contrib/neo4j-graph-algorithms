package org.neo4j.graphalgo.impl.msbfs;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.junit.Assert.assertEquals;


public final class HugeMultiBitSet32Test extends RandomizedTest {

    @Test
    public void shouldSetSingleBitWithOr() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(1, AllocationTracker.EMPTY);
        for (int i = 0; i < 32; i++) {
            bitSet.setBit(0, i);
            int expected = i == 31 ? -1 : ((1 << i + 1) - 1);
            assertEquals("" + i, expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldSetAndOverwriteCompleteBits() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(1, AllocationTracker.EMPTY);
        int i = randomInt();
        bitSet.set(0, i);
        assertEquals(i, bitSet.get(0));
    }

    @Test
    public void shouldIterateToNextSetBit() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldReturnStartNodeIfSet() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(node));
    }

    @Test
    public void shouldReturnMinusOneIfNoMoreSetBits() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 5);
        bitSet.set(node, 42);
        assertEquals(-1, bitSet.nextSetNodeId(node + 1));
    }

    @Test
    public void shouldReturnMinusOneIfEmptyButCheckedFromMiddle() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(10, AllocationTracker.EMPTY);
        assertEquals(-1, bitSet.nextSetNodeId(between(1, 8)));
    }

    @Test
    public void shouldReturnMinusTwoIfEmpty() {
        HugeMultiBitSet32 bitSet = new HugeMultiBitSet32(10, AllocationTracker.EMPTY);
        assertEquals(-2, bitSet.nextSetNodeId(0));
    }
}
