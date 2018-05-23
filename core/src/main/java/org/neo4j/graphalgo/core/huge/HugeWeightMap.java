package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.container.TrackingLongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

final class HugeWeightMap implements HugeWeightMapping {

    private static final long CLASS_MEMORY = shallowSizeOfInstance(HugeWeightMap.class);

    private final int pageShift;
    private final long pageMask;

    private final double defaultValue;

    private Page[] pages;

    HugeWeightMap(Page[] pages, int pageSize, double defaultValue, AllocationTracker tracker) {
        assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = (long) (pageSize - 1);
        this.defaultValue = defaultValue;
        this.pages = pages;
        tracker.add(CLASS_MEMORY + sizeOfObjectArray(pages.length));
    }

    @Override
    public double weight(final long source, final long target) {
        int pageIndex = (int) (source >>> pageShift);
        Page page = pages[pageIndex];
        if (page != null) {
            return page.get((int) (source & pageMask), target, defaultValue);
        }
        return defaultValue;
    }

    public double defaultValue() {
        return defaultValue;
    }

    @Override
    public long release() {
        if (pages != null) {
            long released = CLASS_MEMORY + sizeOfObjectArray(pages.length);
            for (Page page : pages) {
                if (page != null) {
                    released += page.release();
                }
            }
            pages = null;
            return released;
        }
        return 0L;
    }

    static final class Page {
        private static final long CLASS_MEMORY = shallowSizeOfInstance(Page.class);

        private TrackingLongDoubleHashMap[] data;
        private final AllocationTracker tracker;

        Page(int pageSize, AllocationTracker tracker) {
            this.data = new TrackingLongDoubleHashMap[pageSize];
            this.tracker = tracker;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pageSize));
        }

        double get(int localIndex, long target, double defaultValue) {
            TrackingLongDoubleHashMap map = data[localIndex];
            return map != null ? map.getOrDefault(target, defaultValue) : defaultValue;
        }

        void put(int localIndex, long target, double value) {
            mapForIndex(localIndex).put(target, value);
        }

        long release() {
            if (data != null) {
                long released = CLASS_MEMORY + sizeOfObjectArray(data.length);
                for (TrackingLongDoubleHashMap map : data) {
                    if (map != null) {
                        released += map.free();
                    }
                }
                data = null;
                return released;
            }
            return 0L;
        }

        private TrackingLongDoubleHashMap mapForIndex(int localIndex) {
            TrackingLongDoubleHashMap map = data[localIndex];
            if (map == null) {
                map = data[localIndex] = new TrackingLongDoubleHashMap(tracker);
            }
            return map;
        }
    }
}
