package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.HugeWeightMapping;

/**
 * WeightMapping implementation which always returns
 * a given default weight upon invocation
 *
 * @author mknblch
 */
public class HugeNullWeightMap implements HugeWeightMapping {

    private final double defaultValue;

    public HugeNullWeightMap(double defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public double weight(final long source, final long target) {
        return defaultValue;
    }

    @Override
    public double weight(
            final long source,
            final long target,
            final double defaultValue) {
        return defaultValue;
    }
}
