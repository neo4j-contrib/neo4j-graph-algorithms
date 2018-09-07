/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.paged;

import org.apache.lucene.util.LongBitSet;

import java.util.function.BiFunction;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.BYTES_OBJECT_REF;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;


public final class PagedLongLongBitSet extends PagedDataStructure<PagedLongLongBitSet.NestedMap> {

    // TODO
    private static BiFunction<AllocationTracker, Long, PageAllocator.Factory<NestedMap>> ALLOCATOR_FUNCTION =
            (AllocationTracker a, Long l) -> {
                int pageSize = 1024;
                long pageUsage = 1000;

                return PageAllocator.of(
                        pageSize,
                        pageUsage,
                        (tracker) -> new NestedMap(pageSize, l, tracker),
                        new NestedMap[0]);
            };


    public static PagedLongLongBitSet newBitSet(long size, AllocationTracker tracker) {
        return new PagedLongLongBitSet(size, ALLOCATOR_FUNCTION.apply(tracker, size).newAllocator(tracker));
    }

    private PagedLongLongBitSet(long size, PageAllocator<NestedMap> allocator) {
        super(size, allocator);
    }

    public boolean getOrDefault(long index1, long index2, boolean defaultValue) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        return pages[pageIndex].get(indexInPage, index2, defaultValue);
    }

    public void flip(long index1, long index2) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        pages[pageIndex].flip(indexInPage, index2);
    }

    public void put(long index1, long index2, boolean value) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        pages[pageIndex].put(indexInPage, index2, value);
    }

    static final class NestedMap {
        private final LongBitSet[] page;
        private final long nodeCount;
        private final AllocationTracker tracker;

        NestedMap(final int pageSize, final long nodeCount, final AllocationTracker tracker) {
            this.nodeCount = nodeCount;
            this.tracker = tracker;
            page = new LongBitSet[pageSize];
        }

        boolean get(int indexInPage, long index2, boolean defaultValue) {
            LongBitSet map = page[indexInPage];
            return map != null ? map.get(index2) : defaultValue;
        }

        void flip(int indexInPage, long index2) {
            LongBitSet map = mapForIndex(indexInPage);
            map.flip(index2);
        }

        void put(int indexInPage, long index2, boolean value) {
            if (value) {
                mapForIndex(indexInPage).set(index2);
            } else {
                mapForIndex(indexInPage).clear(index2);
            }
        }

        private LongBitSet mapForIndex(int indexInPage) {
            LongBitSet map = page[indexInPage];
            if (map == null) {
                map = createNewPage(indexInPage);
            }
            return map;
        }

        synchronized private LongBitSet createNewPage(final int indexInPage) {
            LongBitSet map;
            map = page[indexInPage];
            if (map == null) {
                map = page[indexInPage] = new LongBitSet(nodeCount);
            }
            return map;
        }
    }
}
