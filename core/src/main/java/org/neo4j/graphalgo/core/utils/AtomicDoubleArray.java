package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author mknblch
 */
public class AtomicDoubleArray {

    private static final long MAX = Double.doubleToLongBits(Double.MAX_VALUE);

    private final AtomicLongArray data;

    public AtomicDoubleArray(int capacity) {
        data = new AtomicLongArray(capacity);
    }

    public double get(int index) {
        return Double.longBitsToDouble(data.get(index));
    }

    public void set(int index, double value) {
        data.set(index, Double.doubleToLongBits(value));
    }

    public void add(int index, double value) {
        data.set(index, Double.doubleToLongBits(value));
    }

    public void addCapped(int index, double value) {

        double result, expected;
        do {
            expected = Double.longBitsToDouble(data.get(index));
            result = expected + value;
            if (result < expected) {
                result = MAX;
            }
        } while (!data.compareAndSet(index, Double.doubleToLongBits(expected), Double.doubleToLongBits(result)));
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
