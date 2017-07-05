package org.neo4j.graphalgo.impl.msbfs;

import java.util.Arrays;

/**
 * Two 32-bit wide BitSets per node.
 * This class represents the fixed-size bit field as described in [1].
 * Where two bit sets are stored in the same cache line to improve locality
 * on sequential access.
 * It is equivalent to a {@code List<List<BitSet>>}, but implemented more
 * efficiently as a sparse {@code long[]}.
 * The {@code ω} parameter is fixed to 32, that is, only 32 sources can be
 * tracked with this bit set.
 * The MS-BFS algorithm runs multiple instances in parallel if the number of
 * sources exceed 32.
 * <p>
 * Bits are composed of a primary or default bit ({@code "visit"}) and an
 * auxiliary bit ({@code "seen"}).
 * <p>
 * [1]: <a href="http://www.vldb.org/pvldb/vol8/p449-then.pdf">The More the Merrier: Efficient Multi-Source Graph Traversal</a>
 */
final class BiMultiBitSet32 {

    private static final long AUX_MASK = 0xFFFFFFFF00000000L;
    private static final long DEF_MASK = 0x00000000FFFFFFFFL;

    private final long[] bits;

    /**
     * Creates a new bit set for {@code nodeCount} nodes.
     *
     * @throws IllegalArgumentException if there isn't enough memory to hold the data.
     */
    BiMultiBitSet32(int nodeCount) {
        try {
            bits = new long[nodeCount];
        } catch (OutOfMemoryError | NegativeArraySizeException e) {
            IllegalArgumentException iae =
                    new IllegalArgumentException("Invalid nodeCount: " + nodeCount);
            iae.addSuppressed(e);
            throw iae;
        }
    }

    /**
     * Returns to combined bits for the node.
     */
    long get(int nodeId) {
        return bits[nodeId];
    }

    /**
     * Sets a particular bit (in [0, 32)) for a node.
     * Only the auxiliary bit is set.
     */
    void setAuxBit(int nodeId, int bit) {
        assert bit < 32;
        bits[nodeId] |= (1L << (bit + 32));
    }

    /**
     * Returns the next node that has some bits at default position  set and
     * is at least {@code fromNodeId}. May return {@code fromNodeId} itself,
     * if it has bits set.
     * If there are no such nodes, return -1.
     * If there aren't any nodes that have any bit set (all sets are empty), return -2.
     */
    int nextSetNodeId(int fromNodeId) {
        for (int i = fromNodeId; i < bits.length; i++) {
            if (((int) bits[i]) != 0) {
                return i;
            }
        }
        return -2 + Integer.signum(fromNodeId);
    }

    /**
     * Builds the set union on the default bit for the given node as per
     * {@code this.bits[nodeId][def] ∪ bits}.
     */
    void union(int nodeId, int bits) {
        this.bits[nodeId] |= (((long) bits) & DEF_MASK);
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
     *
     * Returns the new default bit.
     */
    int unionDifference(int nodeId) {
        long bit = this.bits[nodeId];

        int aux = (int) (bit >>> 32);
        int def = (int) bit;
        def &= ~aux;
        aux |= def;

        bit = ((long) aux << 32) | (((long) def) & DEF_MASK);
        this.bits[nodeId] = bit;

        return def;
    }

    /**
     * Copies the default bit into the given {@code target} {@link MultiBitSet32}.
     * The default bit is reset to 0 after the copying, the auxiliary bit remains.
     */
    void copyInto(final MultiBitSet32 target) {
        int length = bits.length;
        for (int i = 0; i < length; i++) {
            int bit = (int) bits[i];
            target.set(i, bit);
            bits[i] &= AUX_MASK;
        }
    }

    /**
     * Resets all bits to 0.
     */
    void reset() {
        Arrays.fill(bits, 0L);
    }
}
