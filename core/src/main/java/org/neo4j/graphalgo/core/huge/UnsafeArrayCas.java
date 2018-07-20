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

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public final class UnsafeArrayCas {

    private static final long LONGV_BASE;
    private static final int LONGV_SHIFT;

    static {
        UnsafeUtil.assertHasUnsafe();
        LONGV_BASE = (long) UnsafeUtil.arrayBaseOffset(long[].class);
        int scale = UnsafeUtil.arrayIndexScale(long[].class);
        if ((scale & (scale - 1)) != 0) {
            throw new Error("data type scale not a power of two");
        }
        LONGV_SHIFT = 31 - Integer.numberOfLeadingZeros(scale);
    }


    static void increment(long[] array, int index) {
        long offset = checkedByteOffset(array, index);
        UnsafeUtil.getAndAddLong(array, offset, 1L);
    }

    private static long checkedByteOffset(long[] array, int index) {
        if (index < 0 || index >= array.length) {
            throw new ArrayIndexOutOfBoundsException("index " + index);
        }
        return byteOffset(index);
    }

    private static long byteOffset(int index) {
        return ((long) index << LONGV_SHIFT) + LONGV_BASE;
    }

    private UnsafeArrayCas() {
        throw new UnsupportedOperationException("No instances");
    }
}
