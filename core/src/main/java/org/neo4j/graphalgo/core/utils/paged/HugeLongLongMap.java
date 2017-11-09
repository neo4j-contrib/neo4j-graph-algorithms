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

import com.carrotsearch.hppc.AbstractIterator;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.neo4j.graphalgo.core.utils.container.TrackingLongLongHashMap;

import java.util.Arrays;
import java.util.Iterator;

import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;

public final class HugeLongLongMap extends PagedDataStructure<LongLongMap> implements Iterable<LongLongCursor> {

    private static final PageAllocator.Factory<LongLongMap> ALLOCATOR_FACTORY;

    static {
        // we must use same page size as long[] pages in order to load in parallel
        int pageSize = PageUtil.pageSizeFor(Long.BYTES);
        int bufferLength = (int) Math.ceil(pageSize / DEFAULT_LOAD_FACTOR);
        bufferLength = BitUtil.nextHighestPowerOfTwo(bufferLength);
        long bufferUsage = sizeOfLongArray(bufferLength);

        long pageUsage = shallowSizeOfInstance(LongLongHashMap.class);
        pageUsage += bufferUsage; // keys
        pageUsage += bufferUsage; // values

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                TrackingLongLongHashMap::new,
                new LongLongHashMap[0]);
    }

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, HugeLongLongMap.class);
    }

    public static long estimateMemoryUsageOfPages(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size);
    }

    public static HugeLongLongMap newMap(long size, AllocationTracker tracker) {
        return new HugeLongLongMap(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    public static HugeLongLongMap fromPages(
            long size,
            LongLongMap[] pages,
            AllocationTracker tracker) {
        return new HugeLongLongMap(size, pages, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private HugeLongLongMap(long size, PageAllocator<LongLongMap> allocator) {
        super(size, allocator);
    }

    private HugeLongLongMap(long size, LongLongMap[] pages, PageAllocator<LongLongMap> allocator) {
        super(size, pages, allocator);
    }

    public long getOrDefault(long index, long defaultValue) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        LongLongMap page = pages[pageIndex];
        return page != null
                ? page.getOrDefault(index, defaultValue)
                : defaultValue;
    }

    public long put(long index, long value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final LongLongMap page = pages[pageIndex];
        assert page != null : "add should only be used with pre-allocated pages";
        return page.put(index, value);
    }

    public boolean containsKey(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final LongLongMap page = pages[pageIndex];
        return page != null && page.containsKey(index);
    }

    @Override
    public Iterator<LongLongCursor> iterator() {
        return new Iter();
    }

    private final class Iter extends AbstractIterator<LongLongCursor> {

        private final Iterator<LongLongMap> maps;
        private Iterator<LongLongCursor> current;

        private Iter() {
            maps = Arrays.asList(pages).iterator();
            current = maps.next().iterator();
        }

        @Override
        protected LongLongCursor fetch() {
            while (!current.hasNext()) {
                if (!maps.hasNext()) {
                    return done();
                }
                current = maps.next().iterator();
            }
            return current.next();
        }
    }
}
