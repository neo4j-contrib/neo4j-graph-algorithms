package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class TriangleCoefficientResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodeCount;
    public final double coefficient;

    public TriangleCoefficientResult(long loadMillis, long computeMillis, long writeMillis, long nodeCount, double coefficient) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodeCount = nodeCount;
        this.coefficient = coefficient;
    }

    public static class Builder extends AbstractResultBuilder<TriangleCoefficientResult> {

        private long nodeCount;
        private double averageClusteringCoefficient;

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }

        @Override
        public TriangleCoefficientResult build() {
            return new TriangleCoefficientResult(loadDuration, evalDuration, writeDuration, nodeCount, averageClusteringCoefficient);
        }

    }

}
