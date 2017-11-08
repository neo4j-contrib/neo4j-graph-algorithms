package org.neo4j.graphalgo.core.write;

import org.neo4j.graphalgo.core.utils.AtomicFloatingPointArray;

public final class AtomicDoubleArrayTranslator implements PropertyTranslator.OfDouble<AtomicFloatingPointArray> {

    public static final PropertyTranslator<AtomicFloatingPointArray> INSTANCE = new AtomicDoubleArrayTranslator();

    @Override
    public double toDouble(final AtomicFloatingPointArray data, final long nodeId) {
        return data.get((int) nodeId);
    }
}
