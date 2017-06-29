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

    public double get(int index) {
        return (double) data.get(index) / scaleFactor;
    }

    public void add(int index, double value) {
        data.addAndGet(index, (int) (scaleFactor * value));
    }

    public int length() {
        return data.length();
    }
}
