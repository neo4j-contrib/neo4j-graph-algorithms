package org.neo4j.graphalgo.impl.msbfs;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public final class MultiBitSet32Test extends RandomizedTest {

    @Test
    public void shouldSetSingleBitWithOr() {
        MultiBitSet32 bitSet = new MultiBitSet32(1);
        for (int i = 0; i < 32; i++) {
            bitSet.setBit(0, i);
            int expected = i == 31 ? -1 : ((1 << i + 1) - 1);
            assertEquals("" + i, expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldSetAndOverwriteCompleteBits() {
        MultiBitSet32 bitSet = new MultiBitSet32(1);
        int i = randomInt();
        bitSet.set(0, i);
        assertEquals(i, bitSet.get(0));
    }

    @Test
    public void shouldIterateToNextSetBit() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldReturnStartNodeIfSet() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(node));
    }

    @Test
    public void shouldReturnMinusOneIfNoMoreSetBits() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 5);
        bitSet.set(node, 42);
        assertEquals(-1, bitSet.nextSetNodeId(node + 1));
    }

    @Test
    public void shouldReturnMinusOneIfEmptyButCheckedFromMiddle() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        assertEquals(-1, bitSet.nextSetNodeId(between(1, 8)));
    }

    @Test
    public void shouldReturnMinusTwoIfEmpty() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        assertEquals(-2, bitSet.nextSetNodeId(0));
    }
}
