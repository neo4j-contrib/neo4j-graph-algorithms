package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class LouvainResult {

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final long iterations;
    public final long communityCount;

    private LouvainResult(long loadMillis, long computeMillis, long writeMillis, long nodes, long iterations, long communityCount) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.iterations = iterations;
        this.communityCount = communityCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<LouvainResult> {

        private long nodes = 0;
        private long communityCount = 0;
        private long iterations = 1;

        public Builder withIterations(long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder withCommunityCount(long setCount) {
            this.communityCount = setCount;
            return this;
        }

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public LouvainResult build() {
            return new LouvainResult(loadDuration, evalDuration, writeDuration, nodes, iterations, communityCount);
        }
    }
}
