package org.neo4j.graphalgo.core.leightweight;

import org.apache.lucene.util.ArrayUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntSupplier;

/**
 * Abstraction of an array of integer values that can contain more than 2B elements.
 *
 * @author phorn@avantgarde-labs.de
 */
public final class IntArray implements Iterable<IntArray.Cursor> {

    private long size;
    private int[][] pages;

    /**
     * Page size in bytes: 16KB
     */
    private static final int PAGE_SIZE_IN_BYTES = 1 << 14;
    private static final int PAGE_SIZE = PAGE_SIZE_IN_BYTES / Integer.BYTES;
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
    private static final int PAGE_MASK = PAGE_SIZE - 1;

    /**
     * Allocate a new {@link IntArray}.
     * @param size the initial length of the array
     */
    public static IntArray newArray(long size) {
        return new IntArray(size);
    }

    private IntArray(long size) {
        this.size = size;
        pages = new int[numPages(size)][];
        for (int i = 0; i < pages.length; ++i) {
            pages[i] = newIntPage();
        }
    }

    /**
     * Return the length of this array.
     */
    public final long size() {
        return size;
    }

    /**
     * Get an element given its index.
     */
    public int get(long index) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    /**
     * Set a value at the given index and return the previous value.
     */
    public int set(long index, int value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final int[] page = pages[pageIndex];
        final int ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    /**
     * Fill slots between {@code fromIndex} (inclusive) to {@code toIndex} (exclusive) with the value provided by {@code value}.
     */
    public void fill(
            final long fromIndex,
            final long toIndex,
            final IntSupplier value) {
        assert fromIndex <= toIndex : "can only fill positive slice";
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
     * Grows the IntArray to the new size. The existing content will be preserved.
     * If the current size is large enough, this is no-op and no downsizing is happening.
     */
    public void grow(final long newSize) {
        if (size < newSize) {
            final int numPages = numPages(newSize);
            pages = ArrayUtil.grow(pages, numPages);
            for (int i = numPages - 1; i >= 0 && pages[i] == null; --i) {
                pages[i] = newIntPage();
            }
            this.size = newSize;
        }
    }

    public Cursor newCursor() {
        return new Cursor();
    }

    public Cursor cursor(long offset, long length) {
        return cursor(offset, length, newCursor());
    }

    public Cursor cursor(long offset, long length, Cursor reuse) {
        return reuse.init(offset, length);
    }

    @Override
    public Iterator<Cursor> iterator() {
        return new Iter(cursor(0, size));
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

    private static int[] newIntPage() {
        return new int[PAGE_SIZE];
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

    public class Cursor {
        public int[] array;
        public int offset;
        public int length;

        private long from;
        private long to;
        private long size;
        private int fromPage;
        private int toPage;
        private int currentPage;

        private Cursor init(long fromIndex, long length) {
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
                length = (int) Math.min(PAGE_SIZE - offset, size);
                return true;
            }
            if (currentPage < toPage) {
                array = pages[currentPage];
                offset = 0;
                length = PAGE_SIZE;
                return true;
            }
            if (currentPage == toPage) {
                array = pages[currentPage];
                offset = 0;
                length = indexInPage(to - 1) + 1;
                return true;
            }
            return false;
        }

        public <E extends Exception> void forEach(IntAction<E> action) throws E {
            final int[] array = this.array;
            final int limit = length + offset;
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
