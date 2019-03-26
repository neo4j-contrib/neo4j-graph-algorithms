package org.neo4j.graphalgo.core.huge.loader;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.utils.container.TrackingIntDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

final class PagedPropertyMap {

    private static final int PAGE_SHIFT = 14;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

    static PagedPropertyMap of(long size, AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, PAGE_MASK);
        TrackingIntDoubleHashMap[] pages = new TrackingIntDoubleHashMap[numPages];

        tracker.add(shallowSizeOfInstance(HugeLongArray.class));
        tracker.add(sizeOfObjectArray(numPages));

        return new PagedPropertyMap(size, pages, tracker);
    }

    private final long size;
    private final AllocationTracker tracker;
    private TrackingIntDoubleHashMap[] pages;

    private PagedPropertyMap(
            long size,
            TrackingIntDoubleHashMap[] pages,
            AllocationTracker tracker) {
        this.size = size;
        this.pages = pages;
        this.tracker = tracker;
    }

    public double getOrDefault(long index, double defaultValue) {
        assert index < size;
        int pageIndex = pageIndex(index);
        int indexInPage = indexInPage(index);
        IntDoubleMap page = pages[pageIndex];
        return page == null ? defaultValue : page.getOrDefault(indexInPage, defaultValue);
    }

    public void put(long index, double value) {
        assert index < size;
        int pageIndex = pageIndex(index);
        int indexInPage = indexInPage(index);
        TrackingIntDoubleHashMap subMap = subMap(pageIndex);
        subMap.putSync(indexInPage, value);
    }

    private TrackingIntDoubleHashMap subMap(int pageIndex) {
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap != null) {
            return subMap;
        }
        return forceNewSubMap(pageIndex);
    }

    private synchronized TrackingIntDoubleHashMap forceNewSubMap(int pageIndex) {
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap == null) {
            subMap = new TrackingIntDoubleHashMap(tracker);
            pages[pageIndex] = subMap;
        }
        return subMap;
    }

    public long size() {
        return size;
    }

    public long release() {
        if (pages != null) {
            TrackingIntDoubleHashMap[] pages = this.pages;
            this.pages = null;
            long released = sizeOfObjectArray(pages.length);
            for (TrackingIntDoubleHashMap page : pages) {
                if (page != null) {
                    released += page.instanceSize();
                }
            }
            return released;
        }
        return 0L;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }
}
