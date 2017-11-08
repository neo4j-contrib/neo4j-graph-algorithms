package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Atomic double Array implementation
 * @author mknblch
 */
public class AtomicDoubleArray {

    private final AtomicLongArray data;

    /**
     * Create a new AtomicDoubleArray
     * @param capacity its capacity
     */
    public AtomicDoubleArray(int capacity) {
        data = new AtomicLongArray(capacity);
    }

    /**
     * get the value at index
     * @param index the index
     * @return value at index
     */
    public double get(int index) {
        return Double.longBitsToDouble(data.get(index));
    }

    /**
     * Sets the element at position i to the given value.
     * @param index the index
     * @param value the value
     */
    public void set(int index, double value) {
        data.set(index, Double.doubleToLongBits(value));
    }

    /**
     * add argument to value at index. Behaves exactly like standard java double arithmetic.
     * @param index index
     * @param value value to add
     */
    public void add(int index, double value) {
        long newBits, currentBits;
        do {
            currentBits = data.get(index);
            newBits = Double.doubleToLongBits(Double.longBitsToDouble(currentBits) + value);
        } while (!data.compareAndSet(index, currentBits, newBits));
    }

    /**
     * return capacity
     * @return the capacity
     */
    public int length() {
        return data.length();
    }

    /**
     * set all elements to 0
     */
    public void clear() { // TODO parallel ?!
        for (int i = data.length() - 1; i >= 0; i--) {
            data.set(i, 0);
        }
    }
}
