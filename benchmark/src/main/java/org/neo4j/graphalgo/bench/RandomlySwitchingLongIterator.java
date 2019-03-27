package org.neo4j.graphalgo.bench;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.Random;

final class RandomlySwitchingLongIterator implements PrimitiveLongIterator {
    private final PrimitiveLongIterator delegate;
    private final Random random;
    private boolean hasSkipped;
    private long skipped;

    RandomlySwitchingLongIterator(PrimitiveLongIterator delegate, Random random) {
        this.delegate = delegate;
        this.random = random;
    }

    @Override
    public boolean hasNext() {
        return hasSkipped || delegate.hasNext();
    }

    @Override
    public long next() {
        if (hasSkipped) {
            long elem = skipped;
            hasSkipped = false;
            return elem;
        }
        long next = delegate.next();
        if (delegate.hasNext() && random.nextBoolean()) {
            skipped = next;
            hasSkipped = true;
            return delegate.next();
        }
        return next;
    }
}
