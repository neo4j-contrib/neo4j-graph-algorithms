package org.neo4j.graphalgo.core.huge;


import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodeVLongs;

final class AdjacencyCompression {

    static final int CHUNK_SIZE = 64;

    static abstract class IntValue {
        int value;
    }

    private long[] ids;
    private int length;

    AdjacencyCompression() {
        this.ids = new long[0];
    }

    void copyFrom(CompressedLongArray array) {
        if (ids.length < array.length()) {
            ids = new long[array.length()];
        }
        length = array.uncompress(ids);
    }

    void applyDeltaEncoding() {
        Arrays.sort(ids, 0, length);
        length = applyDelta(ids, length);
    }

    int compress(byte[] out) {
        return encodeVLongs(ids, length, out);
    }

    void writeDegree(byte[] out, int offset) {
        int value = length;
        out[    offset] = (byte) (value);
        out[1 + offset] = (byte) (value >>> 8);
        out[2 + offset] = (byte) (value >>> 16);
        out[3 + offset] = (byte) (value >>> 24);
    }

    private int applyDelta(long values[], int length) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L) {
                values[out++] = delta;
            }
        }
        return out;
    }

    void release() {
        ids = null;
        length = 0;
    }
}
