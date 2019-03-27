package org.neo4j.graphalgo.core.utils;

import org.junit.Test;

public final class RandomIntIteratorTest {

    @Test
    public void shouldRandomlyEmitNumbers() {
        testIterator(0, 10);
    }

    @Test
    public void shouldSupportNonZeroStarts() {
        testIterator(13, 37);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFailOnNegativeRange() {
        testIterator(37, 13);
    }

    @Test
    public void shouldEmitLargeSequences() {
        testIterator(1337, 420_000_000);
    }

    private void testIterator(int start, int end) {
        RandomLongIteratorTest.testIterator(
                (long) start, (long) end,
                (v1, v2) -> new RandomIntIterator((int) v1, (int) v2),
                RandomIntIterator::hasNext,
                RandomIntIterator::next);
    }
}
