package org.neo4j.graphalgo.core.utils.paged;

import java.util.Arrays;

public final class DoubleArray extends PagedDataStructure<double[]> {

    private static final PageAllocator.Factory<double[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(double[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, DoubleArray.class);
    }

    public static DoubleArray newArray(long size, AllocationTracker tracker) {
        return new DoubleArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private DoubleArray(long size, PageAllocator<double[]> allocator) {
        super(size, allocator);
    }

    public double get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public double set(long index, double value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final double[] page = pages[pageIndex];
        final double ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    public void fill(double value) {
        for (double[] page : pages) {
            Arrays.fill(page, value);
        }
    }
}
