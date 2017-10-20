package org.neo4j.graphalgo.results;

public class LabelPropagationStats {

    public final long nodes, iterations, loadMillis, computeMillis, writeMillis;
    public final boolean write, didConverge;
    public final String weightProperty, partitionProperty;

    public LabelPropagationStats(
            final long nodes,
            final long iterations,
            final long loadMillis,
            final long computeMillis,
            final long writeMillis,
            final boolean write,
            final boolean didConverge,
            final String weightProperty,
            final String partitionProperty) {
        this.nodes = nodes;
        this.iterations = iterations;
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.write = write;
        this.didConverge = didConverge;
        this.weightProperty = weightProperty;
        this.partitionProperty = partitionProperty;
    }

    public static class Builder extends AbstractResultBuilder<LabelPropagationStats> {

        private long nodes = 0;
        private long iterations = 0;
        private boolean didConverge = false;
        private boolean write;
        private String weightProperty;
        private String partitionProperty;

        public Builder nodes(final long nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder iterations(final long iterations) {
            this.iterations = iterations;
            return this;
        }

        public Builder didConverge(final boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public Builder write(final boolean write) {
            this.write = write;
            return this;
        }

        public Builder weightProperty(final String weightProperty) {
            this.weightProperty = weightProperty;
            return this;
        }

        public Builder partitionProperty(final String partitionProperty) {
            this.partitionProperty = partitionProperty;
            return this;
        }

        public LabelPropagationStats build() {
            return new LabelPropagationStats(
                    nodes,
                    iterations,
                    loadDuration,
                    evalDuration,
                    writeDuration,
                    write,
                    didConverge,
                    weightProperty,
                    partitionProperty);
        }
    }
}
