package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfIntArray;

public final class AtomicIntArray extends PagedDataStructure<AtomicIntegerArray> {

    private static final PageAllocator.Factory<AtomicIntegerArray> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(Integer.BYTES);
        long pageUsage = shallowSizeOfInstance(AtomicIntegerArray.class) + sizeOfIntArray(pageSize);

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new AtomicIntegerArray(pageSize),
                new AtomicIntegerArray[0]);
    }


    public static AtomicIntArray newArray(long size, AllocationTracker tracker) {
        return new AtomicIntArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private AtomicIntArray(
            final long size,
            final PageAllocator<AtomicIntegerArray> allocator) {
        super(size, allocator);
    }

    public int get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex].get(indexInPage);
    }

    public void set(long index, int value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex].set(indexInPage, value);
    }

    public void add(long index, int delta) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex].addAndGet(indexInPage, delta);
    }

    public boolean cas(long index, int expected, int update) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex].compareAndSet(indexInPage, expected, update);
    }
}
