package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.RawValues;

/**
 * single weight cache
 */
public final class WeightMap implements WeightMapping {

    private final int capacity;
    private LongDoubleMap weights;
    private final double defaultValue;
    private final int propertyId;

    public WeightMap(
            int capacity,
            double defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.defaultValue = defaultValue;
        this.weights = new LongDoubleHashMap();
        this.propertyId = propertyId;
    }

    public WeightMap(
            int capacity,
            LongDoubleMap weights,
            double defaultValue,
            int propertyId) {
        this.capacity = capacity;
        this.weights = weights;
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
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
        final double doubleVal = RawValues.extractValue(value, defaultValue);
        if (doubleVal == defaultValue) {
            return;
        }
        put(id, doubleVal);
    }

    private void put(long key, double value) {
        weights.put(key, value);
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
    public LongDoubleMap weights() {
        return weights;
    }

    public int propertyId() {
        return propertyId;
    }

    @Override
    public int size() {
        return weights.size();
    }
}
