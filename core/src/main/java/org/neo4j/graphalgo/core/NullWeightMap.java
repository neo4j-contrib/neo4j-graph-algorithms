package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.WeightMapping;

/**
 * @author mknobloch
 */
public class NullWeightMap implements WeightMapping {

    private final double defaultValue;

    public NullWeightMap(double defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public double get(long id) {
        return defaultValue;
    }

    @Override
    public double get(final long id, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public double get(final int source, final int target) {
        return defaultValue;
    }

    @Override
    public double get(final int id) {
        return defaultValue;
    }

    @Override
    public double get(final int id, final double defaultValue) {
        return defaultValue;
    }

    @Override
    public void set(long id, Object weight) {
        throw new UnsupportedOperationException();
    }
}
