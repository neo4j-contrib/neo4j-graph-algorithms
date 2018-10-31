package org.neo4j.graphalgo.similarity;

public class RleReader {

    private final double[] vector;

    private double value;

    private int index = 0;
    private int count;

    public RleReader(double[] vector) {
        this.vector = vector;
    }

    public void next() {
        if(count > 0) {
            count--;
            return;
        }

        value = vector[index++];

        if(Double.isInfinite(value)) {
            count = (int) vector[index++] - 1;
            value = vector[index++];
        }
    }

    public double value() {
        return value;
    }
}
