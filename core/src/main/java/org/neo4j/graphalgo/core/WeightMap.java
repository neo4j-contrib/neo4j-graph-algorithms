package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import org.neo4j.graphalgo.api.WeightMapping;

/**
 * single weight cache
 */
public final class WeightMap implements WeightMapping {

    private final int capacity;
    private LongDoubleMap weights;
    private final double defaultValue;

    public WeightMap(final int capacity, double defaultValue) {
        this.capacity = capacity;
        this.defaultValue = defaultValue;
        this.weights = new LongDoubleHashMap(capacity);
    }

    public WeightMap(final int capacity, LongDoubleMap weights, double defaultValue) {
        this.capacity = capacity;
        this.weights = weights;
        this.defaultValue = defaultValue;
    }

    /**
     * return the weight for id or defaultValue if unknown
     */
    @Override
    public double get(long id) {
        return weights.getOrDefault(id, defaultValue);
    }

    @Override
    public double get(final long id, final double defaultValue) {
        return weights.getOrDefault(id, defaultValue);
    }

    @Override
    public void set(long id, Object value) {
        final double doubleVal = extractValue(value);
        if (doubleVal == defaultValue) {
            return;
        }
        put(id, doubleVal);
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

        return defaultValue;
    }

    private void put(long key, double value) {
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
