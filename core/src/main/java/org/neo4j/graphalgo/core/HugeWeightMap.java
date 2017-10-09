package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongDoubleMap;

public final class HugeWeightMap implements HugeWeightMapping {

    private HugeLongLongDoubleMap weights;
    private final double defaultValue;

    public HugeWeightMap(long capacity, double defaultValue, AllocationTracker tracker) {
        this.defaultValue = defaultValue;
        this.weights = HugeLongLongDoubleMap.newMap(capacity, tracker);
    }

    @Override
    public double weight(final long source, final long target) {
        return weights.getOrDefault(source, target, defaultValue);
    }

    @Override
    public double weight(
            final long source,
            final long target,
            final double defaultValue) {
        return weights.getOrDefault(source, target, defaultValue);
    }

    public void put(long key1, long key2, Object value) {
        double doubleVal = RawValues.extractValue(value, defaultValue);
        if (doubleVal == defaultValue) {
            return;
        }
        weights.put(key1, key2, doubleVal);
    }

    @Override
    public long release() {
        if (weights != null) {
            long freed = weights.release();
            weights = null;
            return freed;
        }
        return 0L;
    }
}
