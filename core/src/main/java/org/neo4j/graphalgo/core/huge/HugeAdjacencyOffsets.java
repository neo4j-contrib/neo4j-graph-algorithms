package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class HugeAdjacencyOffsets {

    abstract long get(long index);

    abstract long release();

    static HugeAdjacencyOffsets of(long[][] pages, int pageSize) {
        if (pages.length == 1) {
            return new SinglePageOffsets(pages[0]);
        }
        return new PagedOffsets(pages, pageSize);
    }

    private static final class PagedOffsets extends HugeAdjacencyOffsets {

        private final int pageShift;
        private final long pageMask;
        private long[][] pages;

        private PagedOffsets(long[][] pages, int pageSize) {
            assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
            this.pages = pages;
        }

        @Override
        long get(long index) {
            final int pageIndex = (int) (index >>> pageShift);
            final int indexInPage = (int) (index & pageMask);
            return pages[pageIndex][indexInPage];
        }

        @Override
        long release() {
            if (pages != null) {
                long memoryUsed = sizeOfObjectArray(pages.length);
                for (long[] page : pages) {
                    memoryUsed += sizeOfLongArray(page.length);
                }
                pages = null;
                return memoryUsed;
            }
            return 0L;
        }
    }

    private static final class SinglePageOffsets extends HugeAdjacencyOffsets {

        private long[] page;

        private SinglePageOffsets(long[] page) {
            this.page = page;
        }

        @Override
        long get(long index) {
            return page[(int) index];
        }

        @Override
        long release() {
            if (page != null) {
                long memoryUsed = sizeOfLongArray(page.length);
                page = null;
                return memoryUsed;
            }
            return 0L;
        }
    }
}
