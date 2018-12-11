package org.neo4j.graphalgo.similarity;

public class RleReader {

    private final double[] decodedVector;
    private double[] vector;

    private double value;

    private int index = 0;
    private int count;

    public RleReader(int decodedVectorSize) {
        this.decodedVector = new double[decodedVectorSize];
    }

    public void reset(double[] vector) {
        if (this.vector == null || !this.vector.equals(vector)) {
            this.vector = vector;
            reset();
            compute();
        }
    }

    public double[] read() {
        return decodedVector;
    }

    private void next() {
        if (count > 0) {
            count--;
            return;
        }

        value = vector[index++];

        if (value == Double.POSITIVE_INFINITY) {
            count = (int) vector[index++] - 1;
            value = vector[index++];
        }
    }

    private void reset() {
        this.index = 0;
        this.value = -1;
        this.count = -1;
    }

    private void compute() {
        for (int i = 0; i < decodedVector.length; i++) {
            next();
            decodedVector[i] = value;
        }
    }

}
