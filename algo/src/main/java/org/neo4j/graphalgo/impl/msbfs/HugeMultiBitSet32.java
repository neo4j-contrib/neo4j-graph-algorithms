package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.IntArray;

/**
 * 32-bit wide BitSets per node.
 * This class represents the fixed-size bit field as described in [1].
 * It's equivalent to a {@code List<BitSet>}, but implemented more efficiently
 * as a sparse {@code int[]}.
 * The {@code ω} parameter is fixed to 32, that is, only 32 sources can be
 * tracked with this bit set.
 * The MS-BFS algorithm runs multiple instances in parallel if the number of
 * sources exceed 32.
 * <p>
 * [1]: <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">The More the Merrier: Efficient Multi-Source Graph Traversal</a>
 */
final class HugeMultiBitSet32 {

    private final long nodeCount;
    private final IntArray bits;
    private final IntArray.Cursor cursor;
    private final IntArray.BulkAdder bulkAdder;

    /**
     * Creates a new bit set for {@code nodeCount} nodes.
     *
     * @throws IllegalArgumentException if there isn't enough memory to hold the data.
     */
    HugeMultiBitSet32(long nodeCount, AllocationTracker tracker) {
        this.nodeCount = nodeCount;
        try {
            bits = IntArray.newArray(this.nodeCount, tracker);
            cursor = bits.newCursor();
            bulkAdder = bits.newBulkAdder();
        } catch (OutOfMemoryError e) {
            IllegalArgumentException iae =
                    new IllegalArgumentException("Invalid nodeCount: " + nodeCount);
            iae.addSuppressed(e);
            throw iae;
        }
    }

    /**
     * Sets a particular bit (in [0, 32)) for a node.
     */
    void setBit(long nodeId, int bit) {
        assert bit < 32;
        bits.or(nodeId, 1 << bit);
    }

    /**
     * Sets all bits for a node.
     */
    void set(long nodeId, int bit) {
        bits.set(nodeId, bit);
    }

    /**
     * Returns the BitSet for {@code nodeId} as a {@code long}.
     */
    int get(long nodeId) {
        return bits.get(nodeId);
    }

    /**
     * Returns the next node that has some bits set and is at least {@code fromNodeId}.
     * May return {@code fromNodeId} itself, if it has bits set.
     * If there are no such nodes, return -1.
     * If there aren't any nodes that have any bit set (all sets are empty), return -2.
     */
    long nextSetNodeId(long fromNodeId) {
        final IntArray.Cursor cursor = bits.initCursor(
                fromNodeId,
                nodeCount - fromNodeId,
                this.cursor);
        long n = fromNodeId;
        while (cursor.next()) {
            final int[] array = cursor.array;
            final int offset = cursor.offset;
            final int limit = cursor.limit;
            for (int i = offset; i < limit; i++, n++) {
                if (array[i] != 0) {
                    return n;
                }
            }
        }
        return -2 + Long.signum(fromNodeId);
    }

    IntArray.BulkAdder bulkAdder() {
        bulkAdder.init(0, nodeCount);
        return bulkAdder;
    }
}
