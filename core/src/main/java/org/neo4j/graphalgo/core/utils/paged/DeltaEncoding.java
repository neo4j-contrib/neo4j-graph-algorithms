package org.neo4j.graphalgo.core.utils.paged;

public final class DeltaEncoding {

    private static final long[] encodingSizeCache;

    static {
        encodingSizeCache = new long[66];
        for (int i = 0; i < 65; i++) {
            encodingSizeCache[i] = (long) Math.ceil((double) i / 7.0);
        }
        encodingSizeCache[65] = 1L;
    }

    public static long vSize(long value) {
        int bits = Long.numberOfTrailingZeros(Long.highestOneBit(value)) + 1;
        return encodingSizeCache[bits];
    }

    public static int encodeInt(int value, byte[] array, int offset) {
        array[offset++] = (byte) (value >>> 24);
        array[offset++] = (byte) (value >>> 16);
        array[offset++] = (byte) (value >>> 8);
        array[offset++] = (byte) (value);
        return offset;
    }

    public static int encodeVLong(long value, byte[] array, int offset) {
        long i = value;
        while ((i & ~0x7FL) != 0L) {
            array[offset++] = (byte) ((i & 0x7FL) | 0x80L);
            i >>>= 7L;
        }
        array[offset++] = (byte) i;
        return offset;
    }
}
