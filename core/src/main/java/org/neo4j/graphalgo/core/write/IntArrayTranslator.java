package org.neo4j.graphalgo.core.write;

public final class IntArrayTranslator implements PropertyTranslator.OfInt<int[]> {

    public static final PropertyTranslator<int[]> INSTANCE = new IntArrayTranslator();

    @Override
    public int toInt(final int[] data, final long nodeId) {
        return data[(int) nodeId];
    }
}
