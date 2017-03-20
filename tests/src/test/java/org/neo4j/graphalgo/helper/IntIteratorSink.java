package org.neo4j.graphalgo.helper;

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 *         added 06.03.2017.
 */
public class IntIteratorSink implements IntConsumer {

    private final Set<Integer> data = new HashSet<>();

    public IntIteratorSink consume(PrimitiveIntIterator iterator) {

        while (iterator.hasNext()) {
            data.add(iterator.next());
        }
        return this;
    }

    public IntIteratorSink assertContainsAll(int... expected) {

        for (int i : expected) {
            assertContains(i);
        }
        return this;
    }

    public IntIteratorSink assertContains(int expected) {
        if (!data.contains(expected)) {
            throw new AssertionError(expected + " not element of data " + data);
        }
        return this;
    }

    public IntIteratorSink reset() {
        data.clear();
        return this;
    }

    public IntIteratorSink assertSize(int expected) {
        assertEquals(expected, data.size());
        return this;
    }

    @Override
    public void accept(int value) {
        data.add(value);
    }
}
