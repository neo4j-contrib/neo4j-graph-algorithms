package org.neo4j.graphalgo.results;

/**
 * @author mknblch
 */
public class ShortestPathResult {

    public final long loadDuration;
    public final long evalDuration;
    public final long writeDuration;
    public final long nodeCount;
    public final String targetProperty;

    public ShortestPathResult(long loadDuration,
                              long evalDuration,
                              long writeDuration,
                              int nodeCount,
                              String targetProperty) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.nodeCount = nodeCount;
        this.targetProperty = targetProperty;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractResultBuilder<ShortestPathResult> {

        private int nodeCount = 0;
        private String targetProperty = "";

        public Builder withNodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withTargetProperty(String targetProperty) {
            this.targetProperty = targetProperty;
            return this;
        }

        @Override
        public ShortestPathResult build() {
            return new ShortestPathResult(
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    nodeCount,
                    targetProperty);
        }
    }

}
