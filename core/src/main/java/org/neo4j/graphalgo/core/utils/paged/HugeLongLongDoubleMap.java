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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.container.LongLongDoubleHashMap;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.BYTES_OBJECT_REF;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;


public final class HugeLongLongDoubleMap extends PagedDataStructure<LongLongDoubleHashMap> {


    private static final PageAllocator.Factory<LongLongDoubleHashMap> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(BYTES_OBJECT_REF);
        int bufferLength = BitUtil.nextHighestPowerOfTwo(pageSize) << 1;
        long keyUsage = sizeOfLongArray(bufferLength);
        long valueUsage = sizeOfDoubleArray(bufferLength);

        long pageUsage = shallowSizeOfInstance(LongLongDoubleHashMap.class);
        pageUsage += keyUsage; // keys1
        pageUsage += keyUsage; // keys2
        pageUsage += valueUsage; // values

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new LongLongDoubleHashMap(pageSize),
                new LongLongDoubleHashMap[0]);
    }

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, HugeLongLongDoubleMap.class);
    }

    public static HugeLongLongDoubleMap newMap(long size, AllocationTracker tracker) {
        return new HugeLongLongDoubleMap(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private HugeLongLongDoubleMap(long size, PageAllocator<LongLongDoubleHashMap> allocator) {
        super(size, allocator);
    }

    public double getOrDefault(long index1, long index2, double defaultValue) {
        assert index1 < capacity();
        final int pageIndex = pageIndex(index1);
        return pages[pageIndex].getOrDefault(index1, index2, defaultValue);
    }

    public double put(long index1, long index2, double value) {
        assert index1 < capacity();
        final int pageIndex = pageIndex(index1);
        return pages[pageIndex].put(index1, index2, value);
    }
}
