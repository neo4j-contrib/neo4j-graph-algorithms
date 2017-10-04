package org.neo4j.graphalgo.core.write;

import com.carrotsearch.hppc.IntDoubleMap;

public final class OptionalIntDoubleMapTranslator implements PropertyTranslator.OfOptionalDouble<IntDoubleMap> {

    public static final PropertyTranslator<IntDoubleMap> INSTANCE = new OptionalIntDoubleMapTranslator();

    @Override
    public double toDouble(final IntDoubleMap data, final long nodeId) {
        return data.getOrDefault((int) nodeId, -1D);
    }
}
