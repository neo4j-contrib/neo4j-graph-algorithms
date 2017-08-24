package org.neo4j.graphalgo.core.utils.paged;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PagedDataStructure<T> {

    // 32 KB page size
    private static final int PAGE_SIZE_IN_BYTES = 1 << 15;

    final int pageSize;
    private final int pageShift;
    private final int pageMask;
    private final long maxSupportedSize;

    volatile T[] pages;

    private final AtomicLong size = new PaddedAtomicLong();
    private final AtomicLong capacity = new PaddedAtomicLong();
    private final ReentrantLock growLock = new ReentrantLock(true);

    PagedDataStructure(
            long size,
            int sizeOf,
            Class<T> cls) {
        assert isPowerOfTwo(sizeOf);
        pageSize = PAGE_SIZE_IN_BYTES / sizeOf;
        pageShift = Integer.numberOfTrailingZeros(pageSize);
        pageMask = pageSize - 1;

        final int maxIndexShift = Integer.SIZE - 1 + pageShift;
        maxSupportedSize = 1L << maxIndexShift;
        assert size <= maxSupportedSize;
        this.size.set(size);
        int numPages = numPages(size);
        capacity.set(capacityFor(numPages));
        pages = (T[]) Array.newInstance(cls, numPages);
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newPage();
        }
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

    private int numPages(long capacity) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity;
        return (int) numPages;
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

    protected abstract T newPage();

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
            doGrow(newSize, this.pages.length);
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

    private void doGrow(long newSize, int currentNumPages) {
        int numPages = ArrayUtil.oversize(
                numPages(newSize),
                RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        T[] pages = Arrays.copyOf(this.pages, numPages);
        for (int i = currentNumPages; i < numPages; i++) {
            pages[i] = newPage();
        }
        this.pages = pages;
        this.capacity.set(capacityFor(numPages));
    }

    private static boolean isPowerOfTwo(final int value) {
        return value > 0 && ((value & (~value + 1)) == value);
    }
}
