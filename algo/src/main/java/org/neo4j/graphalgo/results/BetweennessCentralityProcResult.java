package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class BetweennessCentralityProcResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long nodes;
    public final Double minCentrality;
    public final Double maxCentrality;
    public final Double sumCentrality;

    private BetweennessCentralityProcResult(Long loadMillis,
                                            Long computeMillis,
                                            Long writeMillis,
                                            Long nodes,
                                            Double centralityMin,
                                            Double centralityMax,
                                            Double centralitySum) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.minCentrality = centralityMin;
        this.maxCentrality = centralityMax;
        this.sumCentrality = centralitySum;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<BetweennessCentralityProcResult> {

        private long nodes = 0;
        private double centralityMin = -1;
        private double centralityMax = -1;
        private double centralitySum = -1;

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
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

        public BetweennessCentralityProcResult build() {
            return new BetweennessCentralityProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    centralityMin,
                    centralityMax,
                    centralitySum);
        }
    }
}
