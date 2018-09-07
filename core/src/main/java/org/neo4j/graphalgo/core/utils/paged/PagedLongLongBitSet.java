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

import com.carrotsearch.hppc.BitSet;


public final class PagedLongLongBitSet extends PagedDataStructure<PagedLongLongBitSet.NestedMap> {

    private static PageAllocator.Factory<NestedMap> ALLOCATOR_FACTORY;

    private static final int PAGE_SIZE = 1024;

    static {
        long pageUsage = MemoryUsage.sizeOfObjectArray(PAGE_SIZE) + MemoryUsage.shallowSizeOfInstance(NestedMap.class);
        ALLOCATOR_FACTORY = PageAllocator.of(
                PAGE_SIZE,
                pageUsage,
                (tracker) -> new NestedMap(PAGE_SIZE),
                new NestedMap[0]);
    }

    public static PagedLongLongBitSet newBitSet(long capacity, AllocationTracker tracker) {
        final NestedMap[] pages = new NestedMap[PageUtil.numPagesFor(capacity, PAGE_SIZE)];
        return new PagedLongLongBitSet(capacity, pages, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private PagedLongLongBitSet(long size, PageAllocator<NestedMap> allocator) {
        super(size, allocator);
    }

    private PagedLongLongBitSet(long size, PagedLongLongBitSet.NestedMap[] pages, PageAllocator<NestedMap> allocator) {
        super(size, pages, allocator);
    }

    public boolean getOrDefault(long index1, long index2, boolean defaultValue) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        final NestedMap page = pages[pageIndex];
        if (null == page) {
            return defaultValue;
        }
        return page.get(indexInPage, index2, defaultValue);
    }

    public void clear() {
        for (NestedMap page : pages) {
            if (null == page) {
                continue; // TODO break ?
            }
            page.clear();
        }
    }

    public void flip(long index1, long index2) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        final NestedMap page = pages[pageIndex];
        if (null == page) {
            put(index1, index2, true);
        } else {
            page.flip(indexInPage, index2);
        }
    }

    public void put(long index1, long index2, boolean value) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);

        NestedMap page = pages[pageIndex];
        if (null == page) {
            pages[pageIndex] = page = super.allocateNewPage();
        }

        page.put(indexInPage, index2, value);
    }

    static final class NestedMap {
        private final BitSet[] page;

        NestedMap(final int pageSize) {
            page = new BitSet[pageSize];
        }

        boolean get(int indexInPage, long index2, boolean defaultValue) {
            BitSet map = page[indexInPage];
            return map != null ? map.get(index2) : defaultValue;
        }

        void flip(int indexInPage, long index2) {
            BitSet map = mapForIndex(indexInPage);
            map.flip(index2);
        }

        void put(int indexInPage, long index2, boolean value) {
            if (value) {
                mapForIndex(indexInPage).set(index2);
            } else {
                mapForIndex(indexInPage).clear(index2);
            }
        }

        private BitSet mapForIndex(int indexInPage) {
            BitSet map = page[indexInPage];
            if (map == null) {
                map = createNewPage(indexInPage);
            }
            return map;
        }

        synchronized private BitSet createNewPage(final int indexInPage) {
            BitSet map;
            map = page[indexInPage];
            if (map == null) {
                map = page[indexInPage] = new BitSet();
            }
            return map;
        }

        public void clear() {
            for (BitSet bitSet : page) {
                if (null != bitSet) {
                    bitSet.clear();
                }
            }
        }
    }
}
