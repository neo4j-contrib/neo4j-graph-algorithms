package org.neo4j.graphalgo.core.leightweight;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * Abstraction of an array of integer values that can contain more than 2B elements.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class IntArray implements Iterable<IntArray.Cursor> {

    /**
     * Page size in bytes: 16KB
     */
    private static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    private static final int PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long PAGE_SHIFT_L = PAGE_SHIFT;

    /**
     * Allocate a new {@link IntArray}.
     * @param size the initial length of the array
     */
    public static IntArray newArray(long size) {
        return new IntArray(size);
    }

    private long size;
    private long capacity;
    private int[][] pages;

    private final AtomicLong allocIdx = new AtomicLong();

    private IntArray(long size) {
        int numPages = numPages(size);
        this.size = 0;
        this.capacity = ((long) numPages) << PAGE_SHIFT_L;
        pages = new int[numPages][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newIntPage(PAGE_SIZE);
        }
    }

    /**
     * Return the size of this array. Indices up to {@code size} have been filled
     * with data.
     */
    public final long size() {
        return size;
    }

    /**
     * Return the capacity of this array. Not all indices up to this value may have
     * sensible data, but it can be safely written up to this index (exclusive).
     */
    public final long capacity() {
        return capacity;
    }

    /**
     * Get an element given its index.
     */
    public int get(long index) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    /**
     * Set a value at the given index and return the previous value.
     * This method does not advance the {@link #size()} or {@link #grow(long)}s the array.
     */
    public int set(long index, int value) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = pages[pageIndex];
        final int ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    /**
     * Fill slots between {@code fromIndex} (inclusive) to {@code toIndex} (exclusive) with the value provided by {@code value}.
     * This method does not advance the {@link #size()} or {@link #grow(long)}s the array.
     */
    public void fill(
            final long fromIndex,
            final long toIndex,
            final IntSupplier value) {
        assert fromIndex <= toIndex : "can only fill positive slice";
        assert fromIndex < capacity;
        assert toIndex < capacity;
        final int fromPage = pageIndex(fromIndex);
        final int toPage = pageIndex(toIndex - 1);
        if (fromPage == toPage) {
            fill(
                    pages[fromPage],
                    indexInPage(fromIndex),
                    indexInPage(toIndex - 1) + 1,
                    value);
        } else {
            fill(pages[fromPage], indexInPage(fromIndex), PAGE_SIZE, value);
            for (int i = fromPage + 1; i < toPage; ++i) {
                fill(pages[i], value);
            }
            fill(pages[toPage], 0, indexInPage(toIndex - 1) + 1, value);
        }
    }

    /**
     * Returns a new BulkAdder that can be used to add multiple values consecutively
     * without the need for capacity checks by pre-allocating enough space on the
     * internal pages. The BulkAdder will not be positioned and is in an
     * invalid state and cannot be used until {@link #allocate(long, BulkAdder)} is called.
     * Only during calls to {@code allocate} does pre-allocation occur.
     */
    BulkAdder newBulkAdder() {
        return new BulkAdder();
    }

    /**
     * Allocated a certain amount of memory in the internal pages,
     * repositions the provided BulkAdder {@code into} to point to this region
     * and return the start offset where the allocation did happen.
     * this method is thread-safe and can be used to allocate something like
     * thread-local slabs of memory. Allocated slabs must be used fully without fragmentation.
     */
    long allocate(long numberOfElements, BulkAdder into) {
        long intoIndex = allocIdx.getAndAdd(numberOfElements);
        into.init(intoIndex, numberOfElements);
        return intoIndex;
    }

    /**
     * Returns the current index up to which data was allocated using the
     * {@link #allocate(long, BulkAdder)} method.
     */
    long allocationIndex() {
        return allocIdx.get();
    }

    /**
     * Return a new cursor that can iterate over this array.
     * The cursor will not be initialized and is in an invalid state and cannot
     * be used until {@link #cursor(long, long, Cursor)} is called.
     */
    public Cursor newCursor() {
        return new Cursor();
    }

    /**
     * Return a new cursor that can iterate over this array.
     * The cursor will be positioned to the index {@code offset} and will
     * iterate over {@code length} elements.
     */
    public Cursor newCursor(long offset, long length) {
        return cursor(offset, length, newCursor());
    }

    /**
     * Reposition an existing cursor and return it.
     * The cursor will be positioned to the index {@code offset} and will
     * iterate over {@code length} elements.
     * The return value is always {@code == reuse}.
     */
    public Cursor cursor(long offset, long length, Cursor reuse) {
        return reuse.init(offset, length);
    }

    @Override
    public Iterator<Cursor> iterator() {
        return new Iter(newCursor(0, size));
    }

    public Iterator<Cursor> iterator(Cursor reuse) {
        return new Iter(reuse.init(0, size));
    }

    public Iterator<Cursor> iterator(Iterator<Cursor> reuse) {
        if (reuse instanceof Iter) {
            Iter iter = (Iter) reuse;
            iter.cursor.init(0, size);
            iter.state = Iter.UNKNOWN;
            return iter;
        }
        return iterator();
    }

    /**
     * Grows the IntArray to the new size. The existing content will be preserved.
     * If the current size is large enough, this is no-op and no downsizing is happening.
     * {@link #size()} will be updated to reflect the new size.
     */
    private synchronized void grow(final long newSize) {
        if (capacity < newSize) {
            final int currentNumPages = pages.length;
            int numPages = numPages(newSize);
            if (numPages > currentNumPages) {
                numPages = ArrayUtil.oversize(
                        numPages,
                        RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                pages = Arrays.copyOf(pages, numPages);
                for (int i = currentNumPages; i < numPages ; i++) {
                    // we don't strip the last page here as we're already somewhat big
                    pages[i] = newIntPage();
                }
                this.capacity = capacityFor(numPages);
            }
        }
        if (size < newSize) {
            size = newSize;
        }
    }

    private static int numPages(long capacity) {
        final long numPages = (capacity + PAGE_MASK) >>> PAGE_SHIFT;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (PAGE_MASK + 1) + " is too small for such as capacity: " + capacity;
        return (int) numPages;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    private static long capacityFor(int numPages) {
        return ((long) numPages) << PAGE_SHIFT_L;
    }

    private static int[] newIntPage() {
        return new int[PAGE_SIZE];
    }

    private static int[] newIntPage(int size) {
        return new int[size];
    }

    private static void fill(int[] array, IntSupplier value) {
        fill(array, 0, array.length, value);
    }

    private static void fill(int[] array, int from, int to, IntSupplier value) {
        for (int i = from; i < to; i++) {
            array[i] = value.getAsInt();
        }
    }

    public interface IntAction<E extends Exception> {
        boolean accept(int value) throws E;
    }

    private abstract class BaseCursor {
        public int[] array;
        public int offset;
        public int limit;

        protected long from;
        protected long to;
        protected long size;
        private int fromPage;
        private int toPage;
        private int currentPage;

        BaseCursor init(long fromIndex, long length) {
            array = null;
            from = fromIndex;
            to = fromIndex + length;
            size = length;
            fromPage = pageIndex(fromIndex);
            toPage = pageIndex(to - 1);
            currentPage = fromPage - 1;
            return this;
        }

        public boolean next() {
            currentPage++;
            if (currentPage == fromPage) {
                array = pages[currentPage];
                offset = indexInPage(from);
                int length = (int) Math.min(PAGE_SIZE - offset, size);
                limit = offset + length;
                return true;
            }
            if (currentPage < toPage) {
                array = pages[currentPage];
                offset = 0;
                limit = offset + PAGE_SIZE;
                return true;
            }
            if (currentPage == toPage) {
                array = pages[currentPage];
                offset = 0;
                int length = indexInPage(to - 1) + 1;
                limit = offset + length;
                return true;
            }
            array = null;
            return false;
        }
    }

    public final class BulkAdder extends BaseCursor {

        BulkAdder init(long fromIndex, long length) {
            grow(fromIndex + length);
            super.init(fromIndex, length);
            next();
            return this;
        }

        public boolean add(int v) {
            int offset = this.offset++;
            if (offset < limit) {
                array[offset] = v;
                return true;
            }
            return next() && add(v);
        }
    }

    public final class Cursor extends BaseCursor {
        Cursor init(long fromIndex, long length) {
            super.init(fromIndex, length);
            return this;
        }

        public <E extends Exception> void forEach(IntAction<E> action) throws E {
            final int[] array = this.array;
            final int limit = this.limit;
            int offset = this.offset;
            //noinspection StatementWithEmptyBody
            while (offset < limit && action.accept(array[offset++]));
        }
    }

    private final class Iter implements Iterator<Cursor> {
        private final static int UNKNOWN = 0;
        private final static int HAS_NEXT = 1;
        private final static int DONE = 2;

        private int state = UNKNOWN;
        private Cursor cursor;

        private Iter(final Cursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            if (state == UNKNOWN) {
                state = cursor.next() ? HAS_NEXT : DONE;
            }
            return state == HAS_NEXT;
        }

        @Override
        public Cursor next() {
            if (!hasNext()) {
                throw new NoSuchElementException("exhausted");
            }
            state = UNKNOWN;
            return cursor;
        }
    }
}
