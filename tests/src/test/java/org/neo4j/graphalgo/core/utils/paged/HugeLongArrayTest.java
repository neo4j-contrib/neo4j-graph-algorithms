package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class HugeLongArrayTest extends RandomizedTest {

    private static final int PS = 16384;

    @Test
    public void shouldSetAndGet() {
        HugeLongArray array = newArray(10);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        assertEquals(value, array.get(index));
    }

    @Test
    public void shouldBinaryOrValues() {
        HugeLongArray array = newArray(10);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        int newValue = between(42, 1337);
        array.or(index, newValue);
        assertEquals(value | newValue, array.get(index));
    }

    @Test
    public void shouldAddToValues() {
        HugeLongArray array = newArray(10);
        int index = between(2, 8);
        int value = between(42, 1337);
        array.set(index, value);
        int newValue = between(42, 1337);
        array.addTo(index, newValue);
        assertEquals(value + newValue, array.get(index));
    }

    @Test
    public void shouldSetAllValues() {
        int size = between(10, 20);
        HugeLongArray array = newArray(size);
        array.setAll(index -> 1L << index);
        for (int index = 0; index < size; index++) {
            assertEquals(1L << index, array.get(index));
        }
    }

    @Test
    public void shouldFillValues() {
        int size = between(10, 20);
        int value = between(42, 1337);
        HugeLongArray array = newArray(size);
        array.fill(value);
        for (int index = 0; index < size; index++) {
            assertEquals(value, array.get(index));
        }
    }

    @Test
    public void shouldReportSize() {
        int size = between(10, 20);
        HugeLongArray array = newArray(size);
        assertEquals(size, array.size());
    }

    @Test
    public void shouldFreeMemoryUsed() {
        int size = between(10, 20);
        final long expected = MemoryUsage.sizeOfLongArray(size);
        HugeLongArray array = newArray(size);
        final long freed = array.release();

        assertThat(freed, anyOf(is(expected), is(expected + 24)));
    }

    @Test
    public void shouldHaveSinglePageCursor() {
        int size = between(100, 200);
        HugeLongArray array = HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        int from = between(0, 50);
        HugeLongArray.Cursor cursor = array.cursor(from, array.newCursor());

        assertTrue(cursor.next());
        assertEquals(from, cursor.offset);
        assertEquals(size, cursor.limit);

        final long[] expected = new long[size - from];
        Arrays.fill(expected, 42L);
        final long[] actual = Arrays.copyOfRange(cursor.array, cursor.offset, cursor.limit);
        assertArrayEquals(expected, actual);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHaveCursorForSinglePage() {
        int size = between(100, 200);
        HugeLongArray array = HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        int from = between(0, 50);
        HugeLongArray.Cursor cursor = array.cursor(from, array.newCursor());

        assertTrue(cursor.next());
        assertEquals(from, cursor.offset);
        assertEquals(size, cursor.limit);

        final long[] expected = new long[size - from];
        Arrays.fill(expected, 42L);
        final long[] actual = Arrays.copyOfRange(cursor.array, cursor.offset, cursor.limit);
        assertArrayEquals(expected, actual);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHaveCursorForMultiplePages() {
        int size = between(100_000, 200_000);
        HugeLongArray array = HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
        array.fill(42L);
        int from = between(0, 50);
        HugeLongArray.Cursor cursor = array.cursor(from, array.newCursor());

        // first page
        assertTrue(cursor.next());
        assertEquals(from, cursor.offset);
        assertEquals(PS, cursor.limit);

        // middle pages
        int pageEnd = PS;
        while (pageEnd + PS < size) {
            assertTrue(cursor.next());
            assertEquals(0, cursor.offset);
            assertEquals(PS, cursor.limit);
            pageEnd += PS;
        }

        // last page
        assertTrue(cursor.next());
        assertEquals(0, cursor.offset);
        assertEquals(size & (PS - 1), cursor.limit);

        assertFalse(cursor.next());
    }

    @Test
    public void shouldHaveCursor() {
        int size = between(100_000, 200_000);
        HugeLongArray array = newArray(size);
        array.setAll(i -> i + 1L);

        long actual = 0L;
        final HugeLongArray.Cursor cursor = array.cursor(0, array.newCursor());
        while (cursor.next()) {
            long[] ar = cursor.array;
            int offset = cursor.offset;
            int limit = cursor.limit;
            for (int i = offset; i < limit; i++) {
                actual += ar[i];
            }
        }

        final long sum = ((long) size * (long) (size + 1)) / 2L;
        assertEquals(actual, sum);
    }

    @Test
    public void shouldReturnEmptyCursorOnInvalidPosition() {
        int size = between(10, 20);
        HugeLongArray array = newArray(size);
        int index = between(size + 1, size + 11);
        final HugeLongArray.Cursor cursor = array.cursor(index, array.newCursor());
        assertFalse(cursor.next());
    }

    @Test
    public void shouldFailCursorOnNegativePosition() {
        int size = between(10, 20);
        HugeLongArray array = newArray(size);
        int index = between(-20, -10);
        try {
            array.cursor(index, array.newCursor());
            fail("unexpected success");
        } catch (AssertionError e) {
            assertEquals("negative index", e.getMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            // pass
        }
    }

    private HugeLongArray newArray(final int size) {
        if (randomBoolean()) {
            return HugeLongArray.newSingleArray(size, AllocationTracker.EMPTY);
        }
        return HugeLongArray.newPagedArray(size, AllocationTracker.EMPTY);
    }
}
