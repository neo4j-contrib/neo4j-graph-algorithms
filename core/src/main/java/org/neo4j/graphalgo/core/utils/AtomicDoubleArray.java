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

    public boolean compareAndSet(int index, double expect, double value) {
        final int iExpect = (int) (scaleFactor * expect);
        final int iValue = (int) (scaleFactor * value);
        return data.compareAndSet(index, iExpect, iValue);
    }

    public double get(int index) {
        return (double) data.get(index) / scaleFactor;
    }

    public void set(int index, double value) {
        data.set(index, (int) (value * scaleFactor));
    }

    public void add(int index, double value) {
        final int iValue = (int) (scaleFactor * value);
        int current, newValue;
        do {
            current = data.get(index);
            newValue = current + iValue;
        } while (!data.compareAndSet(index, current, newValue));
    }
}
