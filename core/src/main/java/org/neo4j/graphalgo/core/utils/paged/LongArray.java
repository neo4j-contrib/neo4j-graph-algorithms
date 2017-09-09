package org.neo4j.graphalgo.core.utils.paged;

import java.util.function.LongSupplier;

public final class LongArray extends PagedDataStructure<long[]> {

    private static final PageAllocator.Factory<long[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(long[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, LongArray.class);
    }

    public static LongArray newArray(long size, AllocationTracker tracker) {
        return new LongArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private LongArray(long size, PageAllocator<long[]> allocator) {
        super(size, allocator);
    }

    public long get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public long set(long index, long value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final long[] page = pages[pageIndex];
        final long ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    public void fill(
            final long fromIndex,
            final long toIndex,
            final LongSupplier value) {
        assert fromIndex <= toIndex : "can only fill positive slice";
        assert fromIndex < capacity();
        assert toIndex < capacity();
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            fill(
                    pages[fromPage],
                    indexInPage(fromIndex),
                    indexInPage(toIndex - 1) + 1,
                    value);
        } else {
            fill(pages[fromPage], indexInPage(fromIndex), pageSize, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(pages[i], value);
            }
            fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    private static void fill(long[] array, LongSupplier value) {
        fill(array, 0, array.length, value);
    }

    private static void fill(
            long[] array,
            int from,
            int to,
            LongSupplier value) {
        for (int i = from; i < to; i++) {
            array[i] = value.getAsLong();
        }
    }
}
