package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class UnionFindResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;
    public final Long nodeCount;
    public final Long setCount;

    private UnionFindResult(Long loadDuration, Long evalDuration, Long writeDuration, Long nodeCount, Long setCount) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
        this.setCount = setCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long loadDuration = -1;
        private long evalDuration = -1;
        private long writeDuration = -1;
        private long nodeCount = 0;
        private long setCount = 0;

        public Builder withSetCount(long setCount) {
            this.setCount = setCount;
            return this;
        }

        public Builder withNodeCount(long nodeCount) {
            this.nodeCount = nodeCount;
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

        public UnionFindResult build() {
            return new UnionFindResult(loadDuration, evalDuration, writeDuration, nodeCount, setCount);
        }
    }
}
