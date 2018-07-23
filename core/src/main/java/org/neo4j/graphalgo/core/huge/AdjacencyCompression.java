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
import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodedVLongSize;

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

    void copyFrom(long[] targets, int length) {
        ids = targets;
        this.length = length;
    }

    int applyDeltaEncodingAndCalculateRequiredBytes() {
        Arrays.sort(ids, 0, length);
        return applyDeltaAndCalculateRequiredBytes(ids, length);
    }

    int compress(byte[] out, int offset) {
        return encodeVLongs(ids, length, out, offset);
    }

    //@formatter:off
    int writeDegree(byte[] out, int offset) {
        int value = length;
        out[    offset] = (byte) (value);
        out[1 + offset] = (byte) (value >>> 8);
        out[2 + offset] = (byte) (value >>> 16);
        out[3 + offset] = (byte) (value >>> 24);
        return 4 + offset;
    }
    //@formatter:on

    private int applyDeltaAndCalculateRequiredBytes(long values[], int length) {
        long value = values[0], delta;
        int in = 1, out = 1, bytes = encodedVLongSize(value);
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L) {
                bytes += encodedVLongSize(delta);
                values[out++] = delta;
            }
        }
        this.length = out;
        return bytes;
    }

    int degree() {
        return length;
    }
}
