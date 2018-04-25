package org.neo4j.graphalgo.core.huge;

final class VarLongDecoding {

    static int decodeDeltaVLongs(
            long startValue,
            byte[] array,
            int offset,
            int limit,
            long[] out) {
        long input, value = 0L;
        int into = 0, shift = 0;
        while (into < limit) {
            input = (long) array[offset++];
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                startValue += value;
                out[into++] = startValue;
                value = 0L;
                shift = 0;
            } else {
                shift += 7;
            }
        }

        return offset;
    }

    static int zigZagUncompress(byte[] array, int limit, long[] out) {
        long input, startValue = 0L, value = 0L;
        int into = 0, offset = 0, shift = 0;
        while (offset < limit) {
            input = (long) array[offset++];
            value += (input & 127L) << shift;
            if ((input & 128L) == 128L) {
                startValue += ((value >>> 1L) ^ -(value & 1L));
                out[into++] = startValue;
                value = 0L;
                shift = 0;
            } else {
                shift += 7;
            }
        }
        return into;
    }

    private VarLongDecoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
