package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.collection.pool.MarshlandPool;

import java.util.function.IntSupplier;

/**
 * Abstraction of an array of integer values that can contain more than 2B elements.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class IntArray extends PagedDataStructure<int[]> {

    private final MarshlandPool<Cursor> cursors = new MarshlandPool<>(this::newCursor);

    private static final PageAllocator.Factory<int[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(int[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, IntArray.class);
    }

    /**
     * Allocate a new {@link IntArray}.
     *
     * @param size the initial length of the array
     */
    public static IntArray newArray(long size, AllocationTracker tracker) {
        return new IntArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private IntArray(long size, PageAllocator<int[]> allocator) {
        super(size, allocator);
    }

    /**
     * Get an element given its index.
     */
    public int get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    /**
     * Set a value at the given index and return the previous value.
     * This method does not advance the {@link #size()} or {@link #grow(long)}s the array.
     */
    public int set(long index, int value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = pages[pageIndex];
        final int ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    /**
     * Adds the value at the given index and return the new value.
     * This method does not advance the {@link #size()} or {@link #grow(long)}s the array.
     */
    public int addTo(long index, int value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = pages[pageIndex];
        return page[indexInPage] += value;
    }


    public void or(long index, int value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] |= value;
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

    /**
     * {@inheritDoc}
     */
    public BulkAdder newBulkAdder() {
        return new BulkAdder();
    }

    /**
     * {@inheritDoc}
     */
    public Cursor newCursor() {
        return new Cursor();
    }

    /**
     * Return a new initCursor that can iterate over this array.
     * The initCursor will be positioned to the index 0 and will
     * iterate over all elements.
     */
    public Cursor cursorForAll() {
        return initCursor(0, size(), cursors.acquire());
    }

    /**
     * Return a new initCursor that can iterate over this data structure.
     * The initCursor will be positioned to the index {@code offset} and will
     * iterate over {@code length} elements.
     */
    public final Cursor cursorFor(long offset, long length) {
        return initCursor(offset, length, cursors.acquire());
    }

    /**
     * Reposition an existing initCursor and return it.
     * The initCursor will be positioned to the index {@code offset} and will
     * iterate over {@code length} elements.
     * The return value is always {@code == reuse}.
     */
    public final Cursor initCursor(long offset, long length, Cursor reuse) {
        reuse.init(offset, length);
        return reuse;
    }

    public final long release() {
        cursors.close();
        return super.release();
    }

    public void returnCursor(Cursor cursor) {
        cursors.release(cursor);
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

        private long from;
        private long to;
        private long size;
        private int fromPage;
        private int toPage;
        private int currentPage;

        void init(long fromIndex, long length) {
            array = null;
            from = fromIndex;
            to = fromIndex + length;
            size = length;
            fromPage = pageIndex(fromIndex);
            toPage = pageIndex(to - 1);
            currentPage = fromPage - 1;
            if (fromPage > toPage) {
                fromPage = -1;
            }
        }

        public final boolean next() {
            if (!setNext(++currentPage, fromPage, toPage)) {
                array = null;
                return false;
            }
            return true;
        }

        private boolean setNext(int current, int from, int to) {
            if (current > to) {
                return false;
            }
            setNextInRange(current, from, to);
            return true;
        }

        private void setNextInRange(int current, int from, int to) {
            if (current == from) {
                loadFirst(current);
            } else if (current < to) {
                loadMiddle(current);
            } else if (current == to) {
                loadLast(current);
            }
        }

        private void loadFirst(int current) {
            array = pages[current];
            offset = indexInPage(from);
            int length = (int) Math.min(pageSize - offset, size);
            limit = offset + length;
        }

        private void loadMiddle(int current) {
            array = pages[current];
            offset = 0;
            limit = offset + pageSize;
        }

        private void loadLast(int current) {
            array = pages[current];
            offset = 0;
            int length = indexInPage(to - 1) + 1;
            limit = offset + length;
        }
    }

    public final class BulkAdder extends BaseCursor {
        @Override
        public final void init(long fromIndex, long length) {
            grow(fromIndex + length);
            super.init(fromIndex, length);
            next();
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
        public <E extends Exception> void forEach(IntAction<E> action) throws
                E {
            final int[] array = this.array;
            final int limit = this.limit;
            int offset = this.offset;
            //noinspection StatementWithEmptyBody
            while (offset < limit && action.accept(array[offset++])) ;
        }
    }
}
