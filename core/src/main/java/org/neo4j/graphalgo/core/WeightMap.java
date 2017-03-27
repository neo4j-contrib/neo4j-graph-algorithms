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

    public WeightMap(final int capacity) {
        this.capacity = capacity;
    }

    public WeightMap(final int capacity, LongDoubleMap weights) {
        this.capacity = capacity;
        this.weights = weights;
    }

    /**
     * return the weight for id or 0.0 if unknown
     */
    @Override
    public double get(long id) {
        return weights.get(id);
    }

    @Override
    public void set(long id, Object value) {
        put(id, extractValue(value));
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
            weights = new LongDoubleHashMap(capacity); // TODO move to ctor
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
