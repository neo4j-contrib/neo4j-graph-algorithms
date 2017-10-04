package org.neo4j.graphalgo.core.write;

public final class DoubleArrayTranslator implements PropertyTranslator.OfDouble<double[]> {

    public static final PropertyTranslator<double[]> INSTANCE = new DoubleArrayTranslator();

    @Override
    public double toDouble(final double[] data, final long nodeId) {
        return data[(int) nodeId];
    }
}
