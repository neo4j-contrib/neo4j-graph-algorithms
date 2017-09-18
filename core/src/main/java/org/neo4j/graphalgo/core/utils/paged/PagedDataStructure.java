package org.neo4j.graphalgo.core.utils.paged;

import org.apache.lucene.util.ArrayUtil;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class PagedDataStructure<T> {

    final int pageSize;
    private final int pageShift;
    private final int pageMask;
    private final long maxSupportedSize;

    volatile T[] pages;

    private final AtomicLong size = new PaddedAtomicLong();
    private final AtomicLong capacity = new PaddedAtomicLong();
    private final ReentrantLock growLock = new ReentrantLock(true);

    private final PageAllocator<T> allocator;

    PagedDataStructure(long size, PageAllocator<T> allocator) {
        pageSize = allocator.pageSize();
        pageShift = Integer.numberOfTrailingZeros(pageSize);
        pageMask = pageSize - 1;

        final int maxIndexShift = Integer.SIZE - 1 + pageShift;
        maxSupportedSize = 1L << maxIndexShift;
        assert size <= maxSupportedSize;
        this.size.set(size);

        this.allocator = allocator;
        pages = allocator.emptyPages();
        setPages(numPages(size), 0);
    }

    PagedDataStructure(long size, T[] pages, PageAllocator<T> allocator) {
        pageSize = allocator.pageSize();
        pageShift = Integer.numberOfTrailingZeros(pageSize);
        pageMask = pageSize - 1;

        final int maxIndexShift = Integer.SIZE - 1 + pageShift;
        maxSupportedSize = 1L << maxIndexShift;

        this.allocator = allocator;
        this.pages = pages;
        this.capacity.set(capacityFor(pages.length));
        this.size.set(size);
    }

    /**
     * Return the size of this data structure. Indices up to {@code size}
     * have been filled with data.
     */
    public final long size() {
        return size.get();
    }

    /**
     * Return the capacity of this data structure. Not all indices up to this value may have
     * sensible data, but it can be safely written up to this index (exclusive).
     */
    public final long capacity() {
        return capacity.get();
    }

    public long release() {
        size.set(0);
        long freed = allocator.estimateMemoryUsage(capacity.getAndSet(0));
        pages = null;
        return freed;
    }

    private int numPages(long capacity) {
        return PageUtil.numPagesFor(capacity, pageShift, pageMask);
    }

    private long capacityFor(int numPages) {
        return ((long) numPages) << pageShift;
    }

    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    final int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    /**
     * Grows the page structure to the new size. The existing content will be preserved.
     * If the current size is large enough, this is no-op and no downsizing is happening.
     */
    final void grow(final long newSize) {
        assert newSize <= maxSupportedSize;
        long cap = capacity.get();
        if (cap >= newSize) {
            growSize(newSize);
            return;
        }
        growLock.lock();
        try {
            cap = capacity.get();
            if (cap >= newSize) {
                growSize(newSize);
                return;
            }
            int numPages = ArrayUtil.oversize(
                numPages(newSize),
                MemoryUsage.BYTES_OBJECT_REF);
            setPages(numPages, this.pages.length);
            growSize(newSize);
        } finally {
            growLock.unlock();
        }
    }

    private void growSize(final long newSize) {
        long size;
        do {
            size = this.size.get();
        } while (size < newSize && !this.size.compareAndSet(size, newSize));
    }

    private void setPages(int numPages, int currentNumPages) {
        T[] pages = Arrays.copyOf(this.pages, numPages);
        for (int i = currentNumPages; i < numPages; i++) {
            pages[i] = allocator.newPage();
        }
        this.pages = pages;
        this.capacity.set(capacityFor(numPages));
    }
}
