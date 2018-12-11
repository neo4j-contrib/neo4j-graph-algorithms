package org.neo4j.graphalgo.similarity;

public class RleTransformer {
    public static final int REPEAT_CUTOFF = 3;

    public static double[] decode(double[] rleVector, int initialSize) {
        RleReader reader = new RleReader(initialSize);
        reader.reset(rleVector);
        return reader.read();
    }
}
