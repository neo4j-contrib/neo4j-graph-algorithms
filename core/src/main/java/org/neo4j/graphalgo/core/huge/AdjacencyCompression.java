/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

    //@formatter:off
    void writeDegree(byte[] out, int offset) {
        int value = length;
        out[    offset] = (byte) (value);
        out[1 + offset] = (byte) (value >>> 8);
        out[2 + offset] = (byte) (value >>> 16);
        out[3 + offset] = (byte) (value >>> 24);
    }
    //@formatter:on

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
