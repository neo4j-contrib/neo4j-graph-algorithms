package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Atomic double Array implementation
 * @author mknblch
 */
public class AtomicFloatArray implements AtomicFloatingPointArray {

    private final AtomicIntegerArray data;

    /**
     * Create a new AtomicDoubleArray
     * @param capacity its capacity
     */
    public AtomicFloatArray(int capacity) {
        data = new AtomicIntegerArray(capacity);
    }

    /**
     * get the value at index
     * @param index the index
     * @return value at index
     */
    @Override
    public double get(int index) {
        return Double.longBitsToDouble(data.get(index));
    }

    /**
     * Sets the element at position i to the given value.
     * @param index the index
     * @param value the value
     */
    @Override
    public void set(int index, double value) {
        data.set(index, Float.floatToIntBits((float) value));
    }

    /**
     * add argument to value at index. Behaves exactly like standard java double arithmetic.
     * @param index index
     * @param value value to add
     */
    @Override
    public void add(int index, double value) {
        int newBits, currentBits;
        do {
            currentBits = data.get(index);
            newBits = Float.floatToIntBits(Float.intBitsToFloat(currentBits) + (float) value);
        } while (!data.compareAndSet(index, currentBits, newBits));
    }

    /**
     * return capacity
     * @return the capacity
     */
    @Override
    public int length() {
        return data.length();
    }

    /**
     * set all elements to 0
     */
    @Override
    public void clear() { // TODO parallel ?!
        for (int i = data.length() - 1; i >= 0; i--) {
            data.set(i, 0);
        }
    }

    /**
     * @see AtomicFloatingPointArray#getPrecision()
     */
    @Override
    public int getPrecision() {
        return AtomicFloatingPointArray.LOW_PRECISION;
    }
}
