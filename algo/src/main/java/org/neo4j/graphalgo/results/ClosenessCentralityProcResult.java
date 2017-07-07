package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class ClosenessCentralityProcResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long nodes;

    private ClosenessCentralityProcResult(Long loadMillis,
                                          Long computeMillis,
                                          Long writeMillis,
                                          Long nodes) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ClosenessCentralityProcResult> {

        private long nodes = 0;
        private double centralityMin = -1;
        private double centralityMax = -1;
        private double centralitySum = -1;

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public ClosenessCentralityProcResult build() {
            return new ClosenessCentralityProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes);
        }
    }
}
