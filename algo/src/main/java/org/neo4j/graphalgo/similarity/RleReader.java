package org.neo4j.graphalgo.similarity;

public class RleReader {

    private double[] vector;

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

        if(value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            count = (int) vector[index++] - 1;
            value = vector[index++];
        }
    }

    public double value() {
        return value;
    }

    public double[] vector () {
        return vector;
    }

    public void reset(double[] vector) {
        this.vector = vector;
        this.index = 0;
        this.value = -1;
        this.count = -1;
    }
}
