package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Node;

public class PageRankScore {

    public final Node node;
    public final Double score;

    public PageRankScore(final Node node, final Double score) {
        this.node = node;
        this.score = score;
    }

    // TODO: return number of relationships as well
    //  the Graph API doesn't expose this value yet
    public static final class Stats {
        public final long nodes, iterations, loadMillis, computeMillis, writeMillis;
        public final double dampingFactor;
        public final boolean write;
        public final String writeProperty;

        Stats(
                long nodes,
                long iterations,
                long loadMillis,
                long computeMillis,
                long writeMillis,
                double dampingFactor,
                boolean write,
                String writeProperty) {
            this.nodes = nodes;
            this.iterations = iterations;
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.dampingFactor = dampingFactor;
            this.write = write;
            this.writeProperty = writeProperty;
        }

        public static final class Builder extends AbstractResultBuilder<Stats> {
            private long nodes;
            private long iterations;
            private double dampingFactor;
            private boolean write;
            private String writeProperty;

            public Builder withNodes(long nodes) {
                this.nodes = nodes;
                return this;
            }

            public Builder withIterations(long iterations) {
                this.iterations = iterations;
                return this;
            }

            public Builder withDampingFactor(double dampingFactor) {
                this.dampingFactor = dampingFactor;
                return this;
            }

            public Builder withWrite(boolean write) {
                this.write = write;
                return this;
            }

            public Builder withProperty(String writeProperty) {
                this.writeProperty = writeProperty;
                return this;
            }

            public PageRankScore.Stats build() {
                return new PageRankScore.Stats(
                        nodes,
                        iterations,
                        loadDuration,
                        evalDuration,
                        writeDuration,
                        dampingFactor,
                        write,
                        writeProperty);
            }
        }
    }
}
