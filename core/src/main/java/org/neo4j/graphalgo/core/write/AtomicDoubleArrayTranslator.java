package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;

public final class AtomicDoubleArrayTranslator implements PropertyTranslator.OfDouble<AtomicDoubleArray> {

    public static final PropertyTranslator<AtomicDoubleArray> INSTANCE = new AtomicDoubleArrayTranslator();

    @Override
    public double toDouble(final AtomicDoubleArray data, final long nodeId) {
        return data.get((int) nodeId);
    }
}
