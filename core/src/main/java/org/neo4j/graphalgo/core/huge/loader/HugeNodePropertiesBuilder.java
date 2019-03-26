package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;

final class HugeNodePropertiesBuilder {

    private final double defaultValue;
    private final int propertyId;
    private final PagedPropertyMap properties;

    public static HugeNodePropertiesBuilder of(
            long numberOfNodes,
            AllocationTracker tracker,
            double defaultValue,
            int propertyId) {
        assert propertyId != StatementConstants.NO_SUCH_PROPERTY_KEY;
        PagedPropertyMap properties = PagedPropertyMap.of(numberOfNodes, tracker);
        return new HugeNodePropertiesBuilder(defaultValue, propertyId, properties);
    }

    private HugeNodePropertiesBuilder(
            final double defaultValue,
            final int propertyId,
            final PagedPropertyMap properties) {
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
        this.properties = properties;
    }

    double defaultValue() {
        return defaultValue;
    }

    int propertyId() {
        return propertyId;
    }

    void set(long index, double value) {
        properties.put(index, value);
    }

    HugeWeightMapping build() {
        return new HugeNodePropertyMap(properties, defaultValue, propertyId);
    }
}
