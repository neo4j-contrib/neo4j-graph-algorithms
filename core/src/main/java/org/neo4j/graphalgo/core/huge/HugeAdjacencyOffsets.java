package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.MemoryUsage;

final class HugeAdjacencyOffsets {

    static HugeAdjacencyOffsets of(long[][] pages, int pageSize, AllocationTracker tracker) {
        long memoryUsed = MemoryUsage.sizeOfObjectArray(pages.length);
        for (long[] page : pages) {
            memoryUsed += MemoryUsage.sizeOfLongArray(page.length);
        }

        tracker.add(MemoryUsage.shallowSizeOfInstance(HugeAdjacencyOffsets.class));
        tracker.add(memoryUsed);

        return new HugeAdjacencyOffsets(pages, pageSize, memoryUsed);
    }

    private final int pageShift;
    private final long pageMask;
    private long[][] pages;
    private final long memoryUsed;

    private HugeAdjacencyOffsets(long[][] pages, int pageSize, long memoryUsed) {
        assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.pageMask = (long) (pageSize - 1);
        this.pages = pages;
        this.memoryUsed = memoryUsed;
    }

    long get(long index) {
        final int pageIndex = (int) (index >>> pageShift);
        final int indexInPage = (int) (index & pageMask);
        return pages[pageIndex][indexInPage];
    }

    public long release() {
        if (pages != null) {
            pages = null;
            return memoryUsed;
        }
        return 0L;
    }
}
