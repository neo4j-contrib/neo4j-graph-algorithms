package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;
import java.util.function.IntToLongFunction;
import java.util.function.LongUnaryOperator;

/**
 * A long-indexable version of a primitive long array ({@code long[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller long-arrays ({@code long[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code long[]} might be used.
 * <p>
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link SparseLongArray} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code long[]} does ({@code 0}).</li>
 * </ul>
 * <p>
 * <h3>Basic Usage</h3>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * HugeLongArray array = HugeLongArray.newArray(arraySize, tracker);
 * array.set(13L, 37L);
 * long value = array.get(13L);
 * // value = 37L
 * {@code}
 * </pre>
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class HugeLongArray {

    /**
     * @return the long value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public long get(long index);

    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void set(long index, long value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void or(long index, final long value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     * @return the now current value after the operation
     */
    abstract public long and(long index, final long value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void addTo(long index, long value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(long[], IntToLongFunction)}.
     */
    abstract public void setAll(LongUnaryOperator gen);

    /**
     * Assigns the specified long value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(long[], long)}.
     */
    abstract public void fill(long value);

    /**
     * Copies the content of this array into the target array.
     * <p>
     * The behavior is identical to {@link System#arraycopy(Object, int, Object, int, int)}.
     */
    abstract public void copyTo(final HugeLongArray dest, long length);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    abstract public long size();

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link Cursor#close()} every cursor instance as well.
     * <p>
     * The amount is not removed from the {@link AllocationTracker} that had been provided in the {@link #newArray(long, AllocationTracker) Constructor}.
     *
     * @return the amount of memory freed, in bytes.
     */
    abstract public long release();

    /**
     * Returns a new {@link Cursor} for this array. The cursor is not positioned and in an invalid state.
     * You must call {@link Cursor#next()} first to position the cursor to a valid state.
     * Obtaining a {@link Cursor} for an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    abstract public Cursor newCursor();

    /**
     * Resets the {@link Cursor} to range from index 0 until {@link #size()}.
     * The returned cursor is not positioned and in an invalid state.
     * You must call {@link Cursor#next()} first to position the cursor to a valid state.
     * The returned cursor might be the reference-same ({@code ==}) one as the provided one.
     * Resetting the {@link Cursor} of an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    abstract public Cursor cursor(Cursor cursor);

    /**
     * Creates a new array if the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static HugeLongArray newArray(long size, AllocationTracker tracker) {
        if (size <= SingleHugeLongArray.PAGE_SIZE) {
            try {
                return SingleHugeLongArray.of(size, tracker);
            } catch (OutOfMemoryError ignored) {
                // OOM is very likely because we just tried to create a single array that is too large
                // in which case we're just going the paged way. If the OOM had any other reason, we're
                // probably triggering it again in the construction of the paged array, where it will be thrown.
            }
        }
        return PagedHugeLongArray.of(size, tracker);
    }

    /* test-only */
    static HugeLongArray newPagedArray(long size, AllocationTracker tracker) {
        return PagedHugeLongArray.of(size, tracker);
    }

    /* test-only */
    static HugeLongArray newSingleArray(int size, AllocationTracker tracker) {
        return SingleHugeLongArray.of(size, tracker);
    }

    /**
     * View of the underlying data, accessible as slices of {@code long[]} arrays.
     * The values are from {@code array[offset]} (inclusive) until {@code array[limit]} (exclusive).
     * The range might match complete array, but that isn't guaranteed.
     * <p>
     * The {@code limit} parameter does not have the same meaning as the {@code length} parameter that is used in many methods that can operate on array slices.
     * The proper value would be {@code int length = limit - offset}.
     */
    public static abstract class Cursor implements AutoCloseable {

        /** the base for the index to get the global index */
        public long base;
        /** a slice of values currently being traversed */
        public long[] array;
        /** the offset into the array */
        public final int offset = 0;
        /** the limit of the array, exclusive – the first index not to be contained */
        public int limit;

        Cursor() {
        }

        /**
         * Try to load the next page and return the success of this load.
         * Once the method returns {@code false}, this method will never return {@code true} again until the cursor is reset using {@link #cursor(Cursor)}.
         * The cursor behavior is not defined and might be unusable and throw exceptions after this method returns {@code false}.
         *
         * @return true, iff the cursor is still valid on contains new data; false if there is no more data.
         */
        abstract public boolean next();

        /**
         * Releases the reference to the underlying array so that it might be garbage collected.
         * The cursor can never be used again after calling this method, doing so results in undefined behavior.
         */
        @Override
        abstract public void close();
    }

    /**
     * A {@link PropertyTranslator} for instances of {@link HugeLongArray}s.
     */
    public static class Translator implements PropertyTranslator.OfLong<HugeLongArray> {

        public static final Translator INSTANCE = new Translator();

        @Override
        public long toLong(final HugeLongArray data, final long nodeId) {
            return data.get(nodeId);
        }
    }

    private static final class SingleHugeLongArray extends HugeLongArray {

        private static final int PAGE_SHIFT = 30;
        private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

        private static HugeLongArray of(long size, AllocationTracker tracker) {
            assert size <= PAGE_SIZE;
            final int intSize = (int) size;
            long[] page = new long[intSize];

            tracker.add(MemoryUsage.shallowSizeOfInstance(HugeLongArray.class));
            tracker.add(MemoryUsage.sizeOfLongArray(intSize));

            return new SingleHugeLongArray(intSize, page);
        }

        private final int size;
        private long[] page;

        private SingleHugeLongArray(int size, long[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public long get(long index) {
            assert index < size;
            return page[(int) index];
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public void or(long index, final long value) {
            assert index < size;
            page[(int) index] |= value;
        }

        @Override
        public long and(long index, final long value) {
            assert index < size;
            return page[(int) index] &= value;
        }

        @Override
        public void addTo(long index, long value) {
            assert index < size;
            page[(int) index] += value;
        }

        @Override
        public void setAll(LongUnaryOperator gen) {
            Arrays.setAll(page, gen::applyAsLong);
        }

        @Override
        public void fill(long value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeLongArray) {
                SingleHugeLongArray dst = (SingleHugeLongArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, 0L);
            } else if (dest instanceof PagedHugeLongArray) {
                PagedHugeLongArray dst = (PagedHugeLongArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (long[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, 0L);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, 0L);
                        }
                        start += toCopy;
                        remaining -= toCopy;
                    }
                }
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return MemoryUsage.sizeOfLongArray(size);
            }
            return 0L;
        }

        @Override
        public Cursor newCursor() {
            return new SingleCursor(page);
        }

        @Override
        public Cursor cursor(final Cursor cursor) {
            assert cursor instanceof SingleCursor;
            ((SingleCursor) cursor).init();
            return cursor;
        }

        private static final class SingleCursor extends Cursor {

            private boolean exhausted;

            private SingleCursor(final long[] page) {
                super();
                this.array = page;
                this.base = 0L;
                this.limit = page.length;
            }

            private void init() {
                exhausted = false;
            }

            public final boolean next() {
                if (exhausted) {
                    return false;
                }
                exhausted = true;
                return true;
            }

            @Override
            public void close() {
                array = null;
                limit = 0;
                exhausted = true;
            }
        }
    }

    public static final class PagedHugeLongArray extends HugeLongArray {

        private static final int PAGE_SHIFT = 14;
        private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
        private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

        private static HugeLongArray of(long size, AllocationTracker tracker) {
            int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
            long[][] pages = new long[numPages][];

            long memoryUsed = MemoryUsage.sizeOfObjectArray(numPages);
            final long pageBytes = MemoryUsage.sizeOfLongArray(PAGE_SIZE);
            for (int i = 0; i < numPages - 1; i++) {
                memoryUsed += pageBytes;
                pages[i] = new long[PAGE_SIZE];
            }
            final int lastPageSize = indexInPage(size);
            pages[numPages - 1] = new long[lastPageSize];
            memoryUsed += MemoryUsage.sizeOfLongArray(lastPageSize);

            tracker.add(MemoryUsage.shallowSizeOfInstance(HugeLongArray.class));
            tracker.add(memoryUsed);

            return new PagedHugeLongArray(size, pages, memoryUsed);
        }

        private final long size;
        private long[][] pages;
        private final long memoryUsed;

        private PagedHugeLongArray(long size, long[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public long get(long index) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void or(long index, final long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] |= value;
        }

        @Override
        public long and(long index, final long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage] &= value;
        }

        @Override
        public void addTo(long index, long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongUnaryOperator gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
                Arrays.setAll(pages[i], j -> gen.applyAsLong(t + j));
            }
        }

        @Override
        public void fill(long value) {
            for (long[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeLongArray) {
                SingleHugeLongArray dst = (SingleHugeLongArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (long[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, 0L);
            } else if (dest instanceof PagedHugeLongArray) {
                PagedHugeLongArray dst = (PagedHugeLongArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    long[] page = pages[i];
                    long[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, 0L);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], 0L);
                }
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long release() {
            if (pages != null) {
                pages = null;
                return memoryUsed;
            }
            return 0L;
        }

        @Override
        public Cursor newCursor() {
            return new PagedCursor(size, pages);
        }

        @Override
        public Cursor cursor(final Cursor cursor) {
            assert cursor instanceof PagedCursor;
            ((PagedCursor) cursor).init();
            return cursor;
        }

        private static int pageIndex(long index) {
            return (int) (index >>> PAGE_SHIFT);
        }

        private static int indexInPage(long index) {
            return (int) (index & PAGE_MASK);
        }

        private static final class PagedCursor extends Cursor {

            private long[][] pages;
            private int maxPage;
            private long capacity;

            private int page;
            private int fromPage;

            private PagedCursor(final long capacity, final long[][] pages) {
                super();
                this.capacity = capacity;
                this.maxPage = pages.length - 1;
                this.pages = pages;
            }

            private void init() {
                fromPage = 0;
                array = pages[0];
                base = 0L;
                limit = (int) Math.min(PAGE_SIZE, capacity);
                page = -1;
            }

            public final boolean next() {
                int current = ++page;
                if (current == fromPage) {
                    return true;
                }
                if (current > maxPage) {
                    return false;
                }
                base += PAGE_SIZE;
                array = pages[current];
                limit = array.length;
                return true;
            }

            @Override
            public void close() {
                array = null;
                pages = null;
                base = 0L;
                limit = 0;
                capacity = 0L;
                maxPage = -1;
                fromPage = -1;
                page = -1;
            }
        }
    }
}
