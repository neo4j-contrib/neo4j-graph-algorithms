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

public final class SparseLongArray extends PagedDataStructure<long[]> {

    public static final long NOT_FOUND = -1L;

    private static final PageAllocator.Factory<long[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(long[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(
                size,
                SparseLongArray.class);
    }

    public static SparseLongArray newArray(
            long size,
            AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, ALLOCATOR_FACTORY.pageSize());
        return fromPages(size, new long[numPages][], tracker);
    }

    public static SparseLongArray fromPages(
            long capacity,
            long[][] pages,
            AllocationTracker tracker) {
        return new SparseLongArray(
                capacity,
                pages,
                ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private SparseLongArray(
            long capacity,
            long[][] pages,
            PageAllocator<long[]> pageAllocator) {
        super(capacity, pages, pageAllocator);
    }

    public long get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        long[] page = pages[pageIndex];
        return page == null ? NOT_FOUND : (page[indexInPage] & Long.MAX_VALUE);
    }

    public void set(long index, long value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        long[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage();
            pages[pageIndex] = page;
        }
        page[indexInPage] = value;
    }

    public boolean contains(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        long[] page = pages[pageIndex];
        return page != null && page[indexInPage] != 0;
    }
}
