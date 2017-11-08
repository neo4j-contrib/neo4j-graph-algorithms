package org.neo4j.graphalgo.core.utils;

/**
 * @author mknblch
 */
public interface AtomicFloatingPointArray {

    /**
     * 64bit precision
     */
    int HIGH_PRECISION = 1;

    /**
     * 32bit precision
     */
    int LOW_PRECISION = 0;

    /**
     * get the value at index
     * @param index the index
     * @return value at index
     */
    double get(int index);

    /**
     * Sets the element at position i to the given value.
     * @param index the index
     * @param value the value
     */
    void set(int index, double value);

    /**
     * add argument to value at index. Behaves exactly like standard java double arithmetic.
     * @param index index
     * @param value value to add
     */
    void add(int index, double value);

    /**
     * return capacity
     * @return the capacity
     */
    int length();

    /**
     * set all elements to 0
     */
    void clear();

    /**
     * return precision of the array.
     * @return either {@link AtomicFloatingPointArray#HIGH_PRECISION} or
     * {@link AtomicFloatingPointArray#LOW_PRECISION}
     */
    int getPrecision();

    static AtomicFloatingPointArray create(int capacity, int precision) {
        switch (precision) {
            case HIGH_PRECISION:
                return new AtomicDoubleArray(capacity);
            case LOW_PRECISION:
                return new AtomicFloatArray(capacity);
            default:
                throw new IllegalArgumentException("Unknown precision " + precision);
        }
    }
}
