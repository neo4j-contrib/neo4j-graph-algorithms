package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;

import java.util.Map;

final class IdsAndProperties {

    final HugeIdMap hugeIdMap;
    final Map<String, HugeWeightMapping> properties;

    IdsAndProperties(
            final HugeIdMap hugeIdMap,
            final Map<String, HugeWeightMapping> properties) {
        this.hugeIdMap = hugeIdMap;
        this.properties = properties;
    }
}
