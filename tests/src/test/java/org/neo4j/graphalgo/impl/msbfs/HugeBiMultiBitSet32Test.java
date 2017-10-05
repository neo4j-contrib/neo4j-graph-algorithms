package org.neo4j.graphalgo.impl.msbfs;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public final class HugeBiMultiBitSet32Test extends RandomizedTest {

    @Test
    public void shouldSetAuxiliaryBit() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(1, AllocationTracker.EMPTY);
        for (int i = 0; i < 32; i++) {
            bitSet.setAuxBit(0, i);
            long expected = i == 31 ? 0xFFFFFFFF00000000L : (((1L << i + 1) - 1) << 32);
            assertEquals("" + i, expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldSetAuxiliaryBitsRange() {
        int nodes = between(2, 64);
        int startNode = between(0, nodes / 2);
        int endNode = between(startNode + 1, Math.min(nodes, startNode + 32));
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(nodes, AllocationTracker.EMPTY);
        bitSet.setAuxBits(startNode, endNode - startNode);
        String msg = String.format(" for range=[%d..%d] of %d nodes", startNode, endNode, nodes);
        for (int i = 0; i < startNode; i++) {
            assertEquals(i + msg, 0L, bitSet.get(i));
        }
        for (int i = startNode; i < endNode; i++) {
            long expected = 1L << (i - startNode + 32);
            assertEquals(i + msg, expected, bitSet.get(i));
        }
        for (int i = endNode; i < nodes; i++) {
            assertEquals(i + msg, 0L, bitSet.get(i));
        }
    }

    @Test
    public void shouldSetAuxiliaryBitsArray() {
        int nodes = between(16, 64);
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(nodes, AllocationTracker.EMPTY);
        testSetAuxBits(nodes, bitSet);
    }

    @Test
    public void shouldResetAuxiliaryBitsArray() {
        int nodes = between(16, 64);
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(nodes, AllocationTracker.EMPTY);
        for (int i = 0; i < nodes; i++) {
            bitSet.setAuxBit(i, between(1, 31));
        }
        testSetAuxBits(nodes, bitSet);
    }

    private void testSetAuxBits(final int nodes, final HugeBiMultiBitSet32 bitSet) {
        int sourceNodeCount = between(nodes / 4, Math.min(32, nodes - 7));
        LongHashSet sources = new LongHashSet(sourceNodeCount);
        while (sources.size() < sourceNodeCount) {
            sources.add(between(4, nodes - 4));
        }

        long[] sourceNodes = sources.toArray();
        Arrays.sort(sourceNodes);
        bitSet.setAuxBits(sourceNodes);

        String msg = String.format(" for sources=[%s] of %d nodes", Arrays.toString(sourceNodes), nodes);
        for (int i = 0; i < nodes; i++) {
            if (sources.contains(i)) {
                int bitPos = Arrays.binarySearch(sourceNodes, i);
                long expected = 1L << (bitPos + 32);
                assertEquals(i + msg, expected, bitSet.get(i));
            } else {
                assertEquals(i + msg, 0L, bitSet.get(i));
            }
        }
    }

    @Test
    public void shouldBuildSetUnionOnDefaultBit() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(1, AllocationTracker.EMPTY);
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
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(1, AllocationTracker.EMPTY);
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
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(1, AllocationTracker.EMPTY);
        bitSet.union(0, Integer.MIN_VALUE);
        long expected = ((long) Integer.MAX_VALUE) + 1;
        assertEquals(expected, bitSet.get(0));
    }

    @Test
    public void shouldIterateToNextSetBit() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 9);
        bitSet.union(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldReturnStartNodeIfSet() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 9);
        bitSet.union(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(node));
    }

    @Test
    public void shouldReturnMinusOneIfNoMoreSetBits() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        int node = between(1, 5);
        bitSet.union(node, 42);
        assertEquals(-1, bitSet.nextSetNodeId(node + 1));
    }

    @Test
    public void shouldReturnMinusOneIfEmptyButCheckedFromMiddle() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        assertEquals(-1, bitSet.nextSetNodeId(between(1, 8)));
    }

    @Test
    public void shouldReturnMinusTwoIfEmpty() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        assertEquals(-2, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldOnlyIterateOverSetDefaultBits() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(10, AllocationTracker.EMPTY);
        bitSet.setAuxBit(between(1, 4), between(1, 31));
        int nextDefBit = between(5, 9);
        bitSet.union(nextDefBit, 42);
        assertEquals(nextDefBit, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldCalculateUnionDifference() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(1, AllocationTracker.EMPTY);
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
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(2, AllocationTracker.EMPTY);
        bitSet.union(0, 42);
        bitSet.union(1, 1337);

        HugeMultiBitSet32 bitSet2 = new HugeMultiBitSet32(2, AllocationTracker.EMPTY);
        bitSet.copyInto(bitSet2);

        assertEquals(0L, bitSet.get(0));
        assertEquals(0L, bitSet.get(1));
        assertEquals(42, bitSet2.get(0));
        assertEquals(1337, bitSet2.get(1));
    }

    @Test
    public void shouldKeepAuxiliaryBitsWhileCopying() {
        HugeBiMultiBitSet32 bitSet = new HugeBiMultiBitSet32(2, AllocationTracker.EMPTY);
        bitSet.setAuxBit(0, 4);
        bitSet.setAuxBit(1, 2);
        bitSet.union(0, 42);
        bitSet.union(1, 1337);

        HugeMultiBitSet32 bitSet2 = new HugeMultiBitSet32(2, AllocationTracker.EMPTY);
        bitSet.copyInto(bitSet2);

        assertEquals(1L << 36, bitSet.get(0));
        assertEquals(1L << 34, bitSet.get(1));
        assertEquals(42, bitSet2.get(0));
        assertEquals(1337, bitSet2.get(1));
    }
}
