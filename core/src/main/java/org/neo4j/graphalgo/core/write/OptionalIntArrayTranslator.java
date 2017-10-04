package org.neo4j.graphalgo.core.write;

public final class OptionalIntArrayTranslator implements PropertyTranslator.OfOptionalInt<int[]> {

    public static final PropertyTranslator<int[]> INSTANCE = new OptionalIntArrayTranslator();

    @Override
    public int toInt(final int[] data, final long nodeId) {
        return data[(int) nodeId];
    }
}
