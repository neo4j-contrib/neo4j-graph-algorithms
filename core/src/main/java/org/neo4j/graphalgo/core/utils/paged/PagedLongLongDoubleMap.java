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

import com.carrotsearch.hppc.LongDoubleMap;
import org.neo4j.graphalgo.core.utils.container.TrackingLongDoubleHashMap;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.BYTES_OBJECT_REF;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;


public final class PagedLongLongDoubleMap extends PagedDataStructure<PagedLongLongDoubleMap.NestedMap> {


    private static final PageAllocator.Factory<NestedMap> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(BYTES_OBJECT_REF);

        // can't really estimate memory usage as it depends on degree distribution
        // just pretend every node is connected to 16 other nodes and go from there
        long perNodeUsage = shallowSizeOfInstance(TrackingLongDoubleHashMap.class)
                + sizeOfLongArray(33)    // keys
                + sizeOfDoubleArray(33); // values

        long pageUsage = shallowSizeOfInstance(NestedMap.class);
        pageUsage += (perNodeUsage * pageSize);

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                (tracker) -> new NestedMap(pageSize, tracker),
                new NestedMap[0]);
    }

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, PagedLongLongDoubleMap.class);
    }

    public static PagedLongLongDoubleMap newMap(long size, AllocationTracker tracker) {
        return new PagedLongLongDoubleMap(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private PagedLongLongDoubleMap(long size, PageAllocator<NestedMap> allocator) {
        super(size, allocator);
    }

    public double getOrDefault(long index1, long index2, double defaultValue) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        return pages[pageIndex].get(indexInPage, index2, defaultValue);
    }

    public void put(long index1, long index2, double value) {
        assert index1 < capacity();
        int pageIndex = pageIndex(index1);
        int indexInPage = indexInPage(index1);
        pages[pageIndex].put(indexInPage, index2, value);
    }

    static final class NestedMap {
        private final LongDoubleMap[] page;
        private final AllocationTracker tracker;

        NestedMap(final int pageSize, final AllocationTracker tracker) {
            this.tracker = tracker;
            page = new LongDoubleMap[pageSize];
        }

        double get(int indexInPage, long index2, double defaultValue) {
            LongDoubleMap map = page[indexInPage];
            return map != null ? map.getOrDefault(index2, defaultValue) : defaultValue;
        }

        void put(int indexInPage, long index2, double value) {
            mapForIndex(indexInPage).put(index2, value);
        }

        private LongDoubleMap mapForIndex(int indexInPage) {
            LongDoubleMap map = page[indexInPage];
            if (map == null) {
                map = createNewPage(indexInPage);
            }
            return map;
        }

        synchronized private LongDoubleMap createNewPage(final int indexInPage) {
            LongDoubleMap map;
            map = page[indexInPage];
            if (map == null) {
                map = page[indexInPage] = new TrackingLongDoubleHashMap(tracker);
            }
            return map;
        }
    }
}
