package org.neo4j.graphalgo.impl.pagerank;

public class DegreeCache {

    public final static DegreeCache EMPTY = new DegreeCache(new double[0], new double[0][0], 0.0);

    private double[] aggregatedDegrees;
    private double[][] weights;
    private double averageDegree;

    public DegreeCache(double[] aggregatedDegrees, double[][] weights, double averageDegree) {
        this.aggregatedDegrees = aggregatedDegrees;
        this.weights = weights;
        this.averageDegree = averageDegree;
    }

    double[] aggregatedDegrees() {
        return aggregatedDegrees;
    }

    double[][] weights() {
        return weights;
    }

    double average() {
        return averageDegree;
    }
}
