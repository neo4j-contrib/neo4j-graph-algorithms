package org.neo4j.graphalgo.similarity;

import java.util.Arrays;

public class RleReader {

    private final double[] decodedVector;
    private double[] vector;

    private double value;

    private int index = 0;
    private int count;

    public RleReader(int decodedVectorSize) {
        this.decodedVector = new double[decodedVectorSize];
    }

    public void next() {
        if (count > 0) {
            count--;
            return;
        }

        value = vector[index++];

        if (value == Double.POSITIVE_INFINITY || value == Double.NEGATIVE_INFINITY) {
            count = (int) vector[index++] - 1;
            value = vector[index++];
        }
    }

    public double value() {
        return value;
    }

    public double[] vector() {
        return vector;
    }

    public void reset(double[] vector) {
        if (this.vector == null || !this.vector.equals(vector)) {
            this.vector = vector;
            this.index = 0;
            this.value = -1;
            this.count = -1;
        }
    }

    public double[] read() {
        if (index == 0) {
            for (int i = 0; i < decodedVector.length; i++) {
                next();
                decodedVector[i] = value();
            }
        }
        return decodedVector;
    }

}
