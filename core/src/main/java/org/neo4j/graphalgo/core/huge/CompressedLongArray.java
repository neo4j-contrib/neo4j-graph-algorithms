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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.VarLongDecoding.zigZagUncompress;
import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodeVLong;
import static org.neo4j.graphalgo.core.huge.VarLongEncoding.encodedVLongSize;
import static org.neo4j.graphalgo.core.huge.VarLongEncoding.zigZag;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfByteArray;

final class CompressedLongArray {

    private final AllocationTracker tracker;
    private byte[] storage;
    private int pos;
    private long lastValue;
    private int length;

    CompressedLongArray(AllocationTracker tracker, long v, int maxLen) {
        this.tracker = tracker;
        int initialLen = maxLen < Integer.MAX_VALUE ? maxLen : 0;
        this.storage = new byte[initialLen];
        tracker.add(sizeOfByteArray(initialLen));
        add(v);
    }

    int add(long v) {
        ++length;
        long value = zigZag(v - lastValue);
        int required = encodedVLongSize(value);
        int pos = this.pos;
        if (storage.length <= pos + required) {
            int newLength = ArrayUtil.oversize(pos + required, Byte.BYTES);
            tracker.remove(sizeOfByteArray(storage.length));
            tracker.add(sizeOfByteArray(newLength));
            storage = Arrays.copyOf(storage, newLength);
        }
        this.pos = encodeVLong(storage, value, pos);
        this.lastValue = v;
        return length;
    }

    int length() {
        return length;
    }

    int uncompress(long[] into) {
        assert into.length >= length;
        return zigZagUncompress(storage, pos, into);
    }

    long compress(
            final AdjacencyCompression compression,
            HugeAdjacencyListBuilder.Allocator allocator) {
        int requiredBytes = compression.compress(storage);
        long address = allocator.allocate(4 + requiredBytes);
        int offset = allocator.offset;
        offset = compression.writeDegree(allocator.page, offset);
        System.arraycopy(storage, 0, allocator.page, offset, requiredBytes);
        allocator.offset = (offset + requiredBytes);
        return address;
    }

    void release() {
        tracker.remove(sizeOfByteArray(storage.length));
        storage = null;
        pos = 0;
        length = 0;
    }
}
