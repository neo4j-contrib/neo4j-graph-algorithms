package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author mknblch
 */
public class AtomicBigDoubleArray {

    private final AtomicLongArray data;

    private final double scaleFactor;

    public AtomicBigDoubleArray(int capacity, double scaleFactor) {
        data = new AtomicLongArray(capacity);
        this.scaleFactor = scaleFactor;
    }

    public boolean compareAndSwap(int index, double expected, double value) {
        return data.compareAndSet(index, (int) (expected * scaleFactor), (int) (value * scaleFactor));
    }

    public double fetchAndAdd(int index, double value) {
        return data.getAndAdd(index, (int) (value * scaleFactor));
    }

    public double fetchCAS(int index, double expected, double value) {
        final long ret = data.get(index);
        compareAndSwap(index, expected, value);
        return ret;
    }

    public double get(int index) {
        return (double) data.get(index) / scaleFactor;
    }

    public void set(int index, double value) {
        data.set(index, (int) (value * scaleFactor));
    }

    public void add(int index, double value) {
        data.addAndGet(index, (int) (scaleFactor * value));
    }

    public void addExact(int index, double value) {
        final long newValue = (long) (value * scaleFactor);
        long result, expected;
        do {
            expected = data.get(index);
            result = Math.addExact(expected, newValue);
        } while (!data.compareAndSet(index, expected, result));
    }

    public void addCapped(int index, double value) {
        final int newValue = (int) (value * scaleFactor);
        long result, expected;
        do {
            expected = data.get(index);
            result = expected + newValue;
            if (result < expected) {
                result = Long.MAX_VALUE;
            }
        } while (!data.compareAndSet(index, expected, result));
    }

    public int length() {
        return data.length();
    }

    public void clear() { // TODO parallel ?!
        for (int i = data.length() - 1; i >= 0; i--) {
            data.set(i, 0);
        }
    }
}
