package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class RandomLongIteratorTest {

    @Test
    public void shouldRandomlyEmitNumbers() {
        testIterator(0L, 10L);
    }

    @Test
    public void shouldSupportNonZeroStarts() {
        testIterator(13L, 37L);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFailOnNegativeRange() {
        testIterator(37L, 13L);
    }

    @Test
    public void shouldEmitLargeSequences() {
        testIterator(1337L, 420_000_000L);
    }

    private void testIterator(long start, long end) {
        testIterator(start, end,
                RandomLongIterator::new,
                RandomLongIterator::hasNext,
                RandomLongIterator::next);
    }

    static <T> void testIterator(
            long start,
            long end,
            BiLongFunction<T> construct,
            Predicate<T> hasNext,
            ToLongFunction<T> next) {
        T iter = construct.apply(start, end);
        long expectedCount = end - start;
        BigInteger expectedBigSum = BigInteger
                .valueOf(expectedCount)
                .multiply(BigInteger.valueOf(start).add(BigInteger.valueOf(end)).subtract(BigInteger.ONE))
                .divide(BigInteger.valueOf(2L));
        long expectedSum = expectedBigSum.longValueExact();
        BitSet seen = new BitSet(end);
        List<Long> underflow = new ArrayList<>();
        List<Long> overflow = new ArrayList<>();
        long collisions = 0L;
        long count = 0L;
        long sum = 0L;
        while (hasNext.test(iter)) {
            long value = next.applyAsLong(iter);
            sum += value;
            ++count;
            if (seen.getAndSet(value)) {
                ++collisions;
            }
            if (value < start) {
                underflow.add(value);
            }
            if (value >= end) {
                overflow.add(value);
            }
        }
        assertEquals("Should not produce collisions, but got " + collisions, 0L, collisions);
        assertEquals("Should emit exactly " + expectedCount + " values, but got " + count, expectedCount, count);
        assertTrue("All values should be greater than " + (start - 1L) + " but got " + underflow, underflow.isEmpty());
        assertTrue("All values should be lower than " + end + " but got " + overflow, overflow.isEmpty());
        assertEquals("Should add up to " + expectedSum + " but got " + sum, expectedSum, sum);
    }
}
