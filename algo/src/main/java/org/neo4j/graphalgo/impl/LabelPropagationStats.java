package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

public class LabelPropagationStats {

    public final long nodes, iterations, loadDuration, evalDuration, writeDuration;
    public final boolean write;
    public final String weightProperty, writeProperty;

    public LabelPropagationStats(
            final long nodes,
            final long iterations,
            final long loadDuration,
            final long evalDuration,
            final long writeDuration,
            final boolean write,
            final String weightProperty,
            final String writeProperty) {
        this.nodes = nodes;
        this.iterations = iterations;
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
        this.write = write;
        this.weightProperty = weightProperty;
        this.writeProperty = writeProperty;
    }

    public static class Builder extends AbstractResultBuilder<LabelPropagationStats> {

        private long nodes = 0;
        private long iterations = 0;
        private boolean write;
        private String weightProperty;
        private String writeProperty;

        public Builder nodes(final long nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder iterations(final long iterations) {
            this.iterations = iterations;
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

        public Builder writeProperty(final String writeProperty) {
            this.writeProperty = writeProperty;
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
                    weightProperty,
                    writeProperty);
        }
    }
}
