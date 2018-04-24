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

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.graphalgo.core.utils.paged.DeltaEncoding;

import java.util.Arrays;

final class CompressedLongArray {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] storage;
    private int pos;
    private long lastValue;
    private int length;

    CompressedLongArray(long v, int maxLen) {
        this.storage = new byte[maxLen];
        add(v);
    }

    int add(long v) {
        ++length;
        long value = v - lastValue;
        int required = DeltaEncoding.singedVSize(value);
        int pos = this.pos;
        if (storage.length <= pos + required) {
            storage = Arrays.copyOf(storage, ArrayUtil.oversize(pos + required, Byte.BYTES));
        }
        this.pos = DeltaEncoding.encodeSignedVLong(value, storage, pos);
        this.lastValue = v;
        return length;
    }

    long[] ensureBufferSize(long[] into) {
        if (into.length <= length) {
            return new long[length];
        }
        return into;
    }

    int uncompress(long[] into) {
        assert into.length >= length;
        byte[] storage = this.storage;
        int offset = 0;
        int limit = pos;
        int out = 0;
        long lastValue = 0;
        while (offset < limit) {
            // offset = DeltaEncoding.decodeSignedVLong(storage, offset, into, out++);
            byte b = storage[offset++];
            long i = (long) ((int) b & 0x7F);
            for (int shift = 7; ((int) b & 0x80) != 0; shift += 7) {
                b = storage[offset++];
                i |= ((long) b & 0x7FL) << shift;
            }
            i = ((i >>> 1) ^ -(i & 1L));
            lastValue += i;
            into[out++] = lastValue;
        }
        return out;
    }

    void release() {
        storage = null;
        pos = 0;
        length = 0;
    }
}
