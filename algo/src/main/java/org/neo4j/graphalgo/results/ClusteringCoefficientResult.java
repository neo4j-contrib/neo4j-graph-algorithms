package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class ClusteringCoefficientResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodeCount;
    public final double coefficient;

    public ClusteringCoefficientResult(long loadMillis, long computeMillis, long writeMillis, long nodeCount, double coefficient) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodeCount = nodeCount;
        this.coefficient = coefficient;
    }

    public static class Builder extends AbstractResultBuilder<ClusteringCoefficientResult> {

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
        public ClusteringCoefficientResult build() {
            return new ClusteringCoefficientResult(loadDuration, evalDuration, writeDuration, nodeCount, averageClusteringCoefficient);
        }

    }

}
