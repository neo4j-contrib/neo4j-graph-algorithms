package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class UnionFindResult {

    public final Long loadMillis;
    public final Long computeMillis;
    public final Long writeMillis;
    public final Long nodes;
    public final Long setCount;

    private UnionFindResult(Long loadMillis, Long computeMillis, Long writeMillis, Long nodes, Long setCount) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.setCount = setCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<UnionFindResult> {

        private long nodes = 0;
        private long setCount = 0;

        public Builder withSetCount(long setCount) {
            this.setCount = setCount;
            return this;
        }

        public Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public UnionFindResult build() {
            return new UnionFindResult(loadDuration, evalDuration, writeDuration, nodes, setCount);
        }
    }
}
