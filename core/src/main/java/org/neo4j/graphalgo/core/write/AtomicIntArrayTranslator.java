package org.neo4j.graphalgo.core.write;

import java.util.concurrent.atomic.AtomicIntegerArray;

public final class AtomicIntArrayTranslator implements PropertyTranslator.OfInt<AtomicIntegerArray> {

    public static final PropertyTranslator<AtomicIntegerArray> INSTANCE = new AtomicIntArrayTranslator();

    @Override
    public int toInt(final AtomicIntegerArray data, final long nodeId) {
        return data.get((int) nodeId);
    }
}
