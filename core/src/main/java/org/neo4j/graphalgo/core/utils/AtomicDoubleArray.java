package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class AtomicDoubleArray {

    private final AtomicIntegerArray data;

    private final double scaleFactor;

    public AtomicDoubleArray(int capacity, double scaleFactor) {
        data = new AtomicIntegerArray(capacity);
        this.scaleFactor = scaleFactor;
    }

    public boolean compareAndSwap(int index, double expected, double value) {
        return data.compareAndSet(index, (int) (expected * scaleFactor), (int) (value * scaleFactor));
    }

    public double fetchAndAdd(int index, double value) {
        return data.getAndAdd(index, (int) (value * scaleFactor));
    }

    public double fetchCAS(int index, double expected, double value) {
        final int ret = data.get(index);
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

    public int length() {
        return data.length();
    }

    public void clear() { // TODO parallel ?!
        for (int i = data.length() - 1; i >= 0; i--) {
            data.set(i, 0);
        }
    }
}
