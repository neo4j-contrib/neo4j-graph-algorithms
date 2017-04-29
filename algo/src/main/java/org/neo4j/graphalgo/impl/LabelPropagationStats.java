package org.neo4j.graphalgo.impl;

public class LabelPropagationStats {

    public final long nodes, iterations, loadMillis, computeMillis, writeMillis;
    public final boolean write;
    public final String weightProperty, partitionProperty;

    public LabelPropagationStats(
            final long nodes,
            final long iterations,
            final long loadMillis,
            final long computeMillis,
            final long writeMillis,
            final boolean write,
            final String weightProperty,
            final String partitionProperty) {
        this.nodes = nodes;
        this.iterations = iterations;
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.write = write;
        this.weightProperty = weightProperty;
        this.partitionProperty = partitionProperty;
    }

    public static class Builder {
        private long nodes = 0;
        private long iterations = 0;
        private long loadMillis = -1;
        private long computeMillis = -1;
        private long writeMillis = -1;
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

        public Builder loadMillis(final long loadMillis) {
            this.loadMillis = loadMillis;
            return this;
        }

        public Builder computeMillis(final long computeMillis) {
            this.computeMillis = computeMillis;
            return this;
        }

        public Builder writeMillis(final long writeMillis) {
            this.writeMillis = writeMillis;
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
                    loadMillis,
                    computeMillis,
                    writeMillis,
                    write,
                    weightProperty,
                    partitionProperty);
        }
    }
}
