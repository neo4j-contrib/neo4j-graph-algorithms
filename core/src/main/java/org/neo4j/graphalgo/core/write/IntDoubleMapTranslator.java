package org.neo4j.graphalgo.core.write;

import com.carrotsearch.hppc.IntDoubleMap;

public final class IntDoubleMapTranslator implements PropertyTranslator.OfDouble<IntDoubleMap> {

    public static final PropertyTranslator<IntDoubleMap> INSTANCE = new IntDoubleMapTranslator();

    @Override
    public double toDouble(final IntDoubleMap data, final long nodeId) {
        return data.get((int) nodeId);
    }
}
