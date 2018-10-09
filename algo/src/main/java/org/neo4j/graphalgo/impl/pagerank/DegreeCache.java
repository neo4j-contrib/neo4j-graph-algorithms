package org.neo4j.graphalgo.impl.pagerank;

public class DegreeCache {

    public final static DegreeCache EMPTY = new DegreeCache(new double[0], new double[0][0]);

    private double[] aggregatedDegrees;
    private double[][] weights;

    public DegreeCache(double[] aggregatedDegrees, double[][] weights) {
        this.aggregatedDegrees = aggregatedDegrees;
        this.weights = weights;
    }

    double[] aggregatedDegrees() {
        return aggregatedDegrees;
    }

    double[][] weights() {
        return weights;
    }

    boolean hasCachedValues() {
        return weights.length > 0;
    }
}
