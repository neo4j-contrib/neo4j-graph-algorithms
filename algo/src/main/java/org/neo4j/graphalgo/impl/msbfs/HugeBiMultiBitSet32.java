package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.IntArray;
import org.neo4j.graphalgo.core.utils.paged.LongArray;

import java.util.Arrays;

final class HugeBiMultiBitSet32 {

    private static final long AUX_MASK = 0xFFFFFFFF00000000L;
    private static final long DEF_MASK = 0x00000000FFFFFFFFL;

    private final LongArray bits;
    private final LongArray.Cursor cursor;

    /**
     * Creates a new bit set for {@code nodeCount} nodes.
     *
     * @throws IllegalArgumentException if there isn't enough memory to hold the data.
     */
    HugeBiMultiBitSet32(long nodeCount, AllocationTracker tracker) {
        try {
            bits = LongArray.newArray(nodeCount, tracker);
            cursor = bits.newCursor();
        } catch (OutOfMemoryError e) {
            IllegalArgumentException iae =
                    new IllegalArgumentException("Invalid nodeCount: " + nodeCount);
            iae.addSuppressed(e);
            throw iae;
        }
    }

    /**
     * Returns to combined bits for the node.
     */
    long get(long nodeId) {
        return bits.get(nodeId);
    }

    /**
     * Sets a particular bit (in [0, 32)) for a node.
     * Only the auxiliary bit is set.
     */
    /* test-only */ void setAuxBit(long nodeId, int bit) {
        assert bit < 32;
        bits.or(nodeId, 1L << (bit + 32));
    }

    /**
     * Resets all bits while setting the aux bits according to the given
     * node range.
     */
    void setAuxBits(long fromId, int len) {
        assert len <= 32;
        assert len >= 1;

        bits.fill(0L);
        for (int i = 0; i < len; i++) {
            bits.set(fromId + i, 1L << (i + 32));
        }
    }

    /**
     * Resets all bits while setting the aux bits according to the given
     * start source node array.
     */
    void setAuxBits(long[] nodes) {
        int len = nodes.length;
        assert len <= 32;
        assert len >= 1;
        assert isSorted(nodes) : "aux bits must be sorted";

        bits.fill(0L);
        for (int i = 0; i < len; i++) {
            bits.set(nodes[i], 1L << (i + 32));
        }
    }

    /**
     * Returns the next node that has some bits at default position  set and
     * is at least {@code fromNodeId}. May return {@code fromNodeId} itself,
     * if it has bits set.
     * If there are no such nodes, return -1.
     * If there aren't any nodes that have any bit set (all sets are empty), return -2.
     */
    long nextSetNodeId(long fromNodeId) {
        final LongArray.Cursor cursor = bits.cursor(fromNodeId, this.cursor);
        long n = fromNodeId;
        while (cursor.next()) {
            final long[] array = cursor.array;
            final int offset = cursor.offset;
            final int limit = cursor.limit;
            for (int i = offset; i < limit; i++, n++) {
                if (((int) array[i]) != 0) {
                    return n;
                }
            }
        }
        return -2 + Long.signum(fromNodeId);
    }

    /**
     * Builds the set union on the default bit for the given node as per
     * {@code this.bits[nodeId][def] ∪ bits}.
     */
    void union(long nodeId, int bits) {
        this.bits.or(nodeId, ((long) bits) & DEF_MASK);
    }


    /**
     * First builds the set difference between the default and the auxiliary bit
     * as per {@code this.bits[nodeId][def] \ this.bits[nodeId][aux]}
     * Then builds the set union between the auxiliary bit and the previously
     * calculated default bit as per {@code this.bits[nodeId][aux] ∪ def}.
     * <p>
     * This is a memory optimized version of the ANP optimization from
     * Listing 4, L15-L16, p455 from the implemented paper.
     * <p>
     * <pre>
     *   15 visitNext[vi] ← visitNext[vi] & ∼seen[vi]
     *   16 seen[vi] ← seen[vi] | visitNext[vi]
     * </pre>
     * <p>
     * In terms of the algorithm, it first removes all BFSs from {@code visitNext}
     * that have already been visited ({@code visitNext &= ~seen}) and then adds
     * all BFSs that visit the node to the seen list ({@code seen |= visitNext}).
     * <p>
     * Returns the new default bit.
     */
    int unionDifference(long nodeId) {
        long bit = this.bits.get(nodeId);

        int aux = (int) (bit >>> 32);
        int def = (int) bit;
        def &= ~aux;
        aux |= def;

        bit = ((long) aux << 32) | (((long) def) & DEF_MASK);
        this.bits.set(nodeId, bit);

        return def;
    }

    /**
     * Copies the default bit into the given {@code target} {@link MultiBitSet32}.
     * The default bit is reset to 0 after the copying, the auxiliary bit remains.
     *
     * @return true iff some data was copied, false otherwise.
     */
    boolean copyInto(final HugeMultiBitSet32 target) {
        boolean didCopy = false;
        final LongArray.Cursor cursor = bits.cursor(0, this.cursor);
        final IntArray.BulkAdder bulkAdder = target.bulkAdder();
        while (cursor.next()) {
            final long[] array = cursor.array;
            final int offset = cursor.offset;
            final int limit = cursor.limit;
            for (int i = offset; i < limit; ++i) {
                int bit = (int) (array[i] & DEF_MASK);
                didCopy = didCopy || bit != 0;
                bulkAdder.add(bit);
                array[i] &= AUX_MASK;
            }
        }
        return didCopy;
    }

    /* assert-only */ private boolean isSorted(long[] nodes) {
        long[] copy = Arrays.copyOf(nodes, nodes.length);
        Arrays.sort(copy);
        return Arrays.equals(copy, nodes);
    }
}
