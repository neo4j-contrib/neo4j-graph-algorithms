package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class BetweennessCentralityProcResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Long nodeCount;
    public final Double minCentrality;
    public final Double maxCentrality;
    public final Double sumCentrality;

    private BetweennessCentralityProcResult(Long loadDuration,
                                            Long evalDuration,
                                            Long writeDuration,
                                            Long nodeCount,
                                            Double centralityMin,
                                            Double centralityMax,
                                            Double centralitySum) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
        this.minCentrality = centralityMin;
        this.maxCentrality = centralityMax;
        this.sumCentrality = centralitySum;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long nodeCount = 0;
        private long loadDuration = -1;
        private long evalDuration = -1;
        private long writeDuration = -1;
        private double centralityMin = -1;
        private double centralityMax = -1;
        private double centralitySum = -1;

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withCentralityMin(double centralityMin) {
            this.centralityMin = centralityMin;
            return this;
        }

        public Builder withCentralityMax(double centralityMax) {
            this.centralityMax = centralityMax;
            return this;
        }

        public Builder withCentralitySum(double centralitySum) {
            this.centralitySum = centralitySum;
            return this;
        }

        public Builder withLoadDuration(long loadDuration) {
            this.loadDuration = loadDuration;
            return this;
        }

        public Builder withEvalDuration(long evalDuration) {
            this.evalDuration = evalDuration;
            return this;
        }

        public Builder withWriteDuration(long writeDuration) {
            this.writeDuration = writeDuration;
            return this;
        }

        public BetweennessCentralityProcResult build() {
            return new BetweennessCentralityProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    centralityMin,
                    centralityMax,
                    centralitySum);
        }
    }
}
