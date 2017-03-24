package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;

/**
 * single weight cache
 */
public final class WeightMapping {

    private final int capacity;
    private LongDoubleMap weights;

    public WeightMapping(final int capacity) {
        this.capacity = capacity;
    }

    public WeightMapping(final int capacity, LongDoubleMap weights) {
        this.capacity = capacity;
        this.weights = weights;
    }

    /**
     * return the weight for id or 0.0 if unknown
     */
    public double get(long id) {
        if (weights != null) {
            return weights.get(id);
        }
        return 0d;
    }

    /**
     * add weight
     */
    public void add(long id, Object value) {
        add(id, extractValue(value));
    }

    /**
     * add weight
     */
    public void add(long id, double weight) {
        if (weight != 0) {
            put(id, weight);
        }
    }

    private double extractValue(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            return number.doubleValue();
        }
        if (value instanceof String) {
            String s = (String) value;
            if (!s.isEmpty()) {
                return Double.parseDouble(s);
            }
        }
        if (value instanceof Boolean) {
            if ((Boolean) value) {
                return 1d;
            }
        }
        // TODO: arrays

        return 0d;
    }

    private void put(long key, double value) {
        if (weights == null) {
            weights = new LongDoubleHashMap(capacity);
        }
        weights.addTo(key, value);
    }

    /**
     * return the capacity
     */
    int capacity() {
        return capacity;
    }

    /**
     * return primitive map for the weights
     */
    LongDoubleMap weights() {
        return weights;
    }
}
