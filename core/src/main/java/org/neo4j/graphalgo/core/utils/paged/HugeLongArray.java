package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntToLongFunction;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

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
     */
    abstract public Cursor newCursor();

    /**
     * Resets the {@link Cursor} to a new position, beginning from the provided index until {@link #size()}.
     * The returned cursor is not positioned and in an invalid state.
     * You must call {@link Cursor#next()} first to position the cursor to a valid state.
     * The returned cursor might be the reference-same ({@code ==}) one as the provided one.
     */
    abstract public Cursor cursor(long from, Cursor cursor);

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
     * Returns a {@link LongStream} of the underlying data.
     * <p>
     * The behavior is the same as for {@link Arrays#stream(long[])}.
     */
    public final LongStream toStream() {
        final Spliterator.OfLong spliter = LongCursorSpliterator.of(this);
        return StreamSupport.longStream(spliter, false);
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

        public long[] array;
        public int offset;
        public int limit;

        Cursor() {
        }

        /**
         * Try to load the next page and return the success of this load.
         * Once the method returns {@code false}, this method will never return {@code true} again until the cursor is reset using {@link #cursor(long, Cursor)}.
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

    static final class LongCursorSpliterator implements Spliterator.OfLong {
        private final Cursor cursor;
        private final int characteristics;

        static Spliterator.OfLong of(HugeLongArray array) {
            Cursor cursor = array.cursor(0, array.newCursor());
            if (cursor.next()) {
                return new LongCursorSpliterator(cursor);
            }
            return Spliterators.emptyLongSpliterator();

        }

        private LongCursorSpliterator(Cursor cursor) {
            this.cursor = cursor;
            this.characteristics = Spliterator.ORDERED | Spliterator.IMMUTABLE;
        }

        @Override
        public OfLong trySplit() {
            final long[] array = cursor.array;
            final int offset = cursor.offset;
            final int limit = cursor.limit;
            if (cursor.next()) {
                return Spliterators.spliterator(array, offset, limit, characteristics);
            }
            return null;
        }

        @Override
        public void forEachRemaining(LongConsumer action) {
            do {
                final long[] array = cursor.array;
                final int offset = cursor.offset;
                final int limit = cursor.limit;
                for (int i = offset; i < limit; i++) {
                    action.accept(array[i]);
                }
            } while (cursor.next());
        }

        @Override
        public boolean tryAdvance(LongConsumer action) {
            do {
                final int index = cursor.offset++;
                if (index < cursor.limit) {
                    action.accept(cursor.array[index]);
                    return true;
                }
            } while (cursor.next());
            return false;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return characteristics;
        }

        @Override
        public Comparator<? super Long> getComparator() {
            return null;
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
        public Cursor cursor(final long from, final Cursor cursor) {
            assert cursor instanceof SingleCursor;
            ((SingleCursor) cursor).init(from);
            return cursor;
        }

        private static final class SingleCursor extends Cursor {

            private boolean exhausted;

            private SingleCursor(final long[] page) {
                super();
                this.array = page;
                this.limit = page.length;
            }

            private void init(long fromIndex) {
                assert fromIndex >= 0 : "negative index";
                if (fromIndex < limit) {
                    offset = (int) fromIndex;
                    exhausted = false;
                } else {
                    offset = limit;
                    exhausted = true;
                }
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
                offset = 0;
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
        public Cursor cursor(final long from, final Cursor cursor) {
            assert cursor instanceof PagedCursor;
            ((PagedCursor) cursor).init(from);
            return cursor;
        }

        private static int pageIndex(long index) {
            return (int) (index >>> PAGE_SHIFT);
        }

        private static int indexInPage(long index) {
            return (int) (index & PAGE_MASK);
        }

        private static final class PagedCursor extends Cursor {

            private static final long[] EMPTY = new long[0];

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

            private void init(long fromIndex) {
                assert fromIndex >= 0 : "negative index";
                if (fromIndex < capacity) {
                    fromPage = pageIndex(fromIndex);
                    array = pages[fromPage];
                    offset = indexInPage(fromIndex);
                    limit = (int) Math.min(PAGE_SIZE, capacity);
                    page = fromPage - 1;
                } else {
                    page = maxPage;
                    array = EMPTY;
                    offset = 0;
                    limit = 0;
                }
            }

            public final boolean next() {
                int current = ++page;
                if (current == fromPage) {
                    return true;
                }
                if (current > maxPage) {
                    return false;
                }
                array = pages[current];
                offset = 0;
                limit = array.length;
                return true;
            }

            @Override
            public void close() {
                array = null;
                offset = 0;
                limit = 0;

                pages = null;
                capacity = 0L;
                maxPage = -1;
                fromPage = -1;
                page = -1;
            }
        }
    }
}
