package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.Random;

/**
 * iterates over a range of long values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * If created with {@link #RandomLongIterable(long, long, Random, boolean)} and {@code true} as last parameter,
 * the iterator returned on every call to {@link #iterator()} will be the same
 * and will provide the same iteration order and cannot be shared.
 */
public final class RandomLongIterable implements PrimitiveLongIterable {

    private final long start;
    private final long end;
    private final Random random;
    private final RandomLongIterator iterator;

    /**
     * @param start  iteration start, inclusive
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomLongIterable(long start, long end, Random random) {
        this(start, end, random, false);
    }

    /**
     * @param start         iteration start, inclusive
     * @param end           iteration end, exclusive
     * @param random        random instance to provide the initial seed
     * @param reuseIterator if true, the iterator returned on every call to {@link #iterator()} will be the same
     *                      and will provide the same iteration order and cannot be shared.
     */
    public RandomLongIterable(long start, long end, Random random, boolean reuseIterator) {
        this.start = start;
        this.end = end;
        this.random = random;
        iterator = reuseIterator ? new RandomLongIterator(start, end, random) : null;
    }

    @Override
    public PrimitiveLongIterator iterator() {
        RandomLongIterator iterator = this.iterator;
        if (iterator == null) {
            iterator = new RandomLongIterator(start, end, random);
        } else {
            iterator.reset();
        }
        return iterator;
    }

    public RandomLongIterator iterator(Random newRandom) {
        return new RandomLongIterator(start, end, newRandom);
    }
}
