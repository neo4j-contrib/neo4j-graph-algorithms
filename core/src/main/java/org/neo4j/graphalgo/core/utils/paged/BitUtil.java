package org.neo4j.graphalgo.core.utils.paged;

public final class BitUtil {

    public static boolean isPowerOfTwo(final int value) {
        return value > 0 && ((value & (~value + 1)) == value);
    }

    public static long nearbyPowerOfTwo(long x) {
        long next = nextHighestPowerOfTwo(x);
        long prev = next >>> 1;
        return (next - x) <= (x - prev) ? next : prev;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static long nextHighestPowerOfTwo(long v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        v++;
        return v;
    }

    private BitUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
