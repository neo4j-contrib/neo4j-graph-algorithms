package org.neo4j.graphalgo.impl;

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
        public final String property;

        Stats(
                long nodes,
                long iterations,
                long loadMillis,
                long computeMillis,
                long writeMillis,
                double dampingFactor,
                boolean write,
                String property) {
            this.nodes = nodes;
            this.iterations = iterations;
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.dampingFactor = dampingFactor;
            this.write = write;
            this.property = property;
        }

        public static final class Builder {
            private long nodes;
            private long iterations;
            private long loadMillis = -1;
            private long computeMillis = -1;
            private long writeMillis = -1;
            private double dampingFactor;
            private boolean write;
            private String property;

            public Builder withNodes(long nodes) {
                this.nodes = nodes;
                return this;
            }

            public Builder withIterations(long iterations) {
                this.iterations = iterations;
                return this;
            }

            public Builder withLoadMillis(long loadMillis) {
                this.loadMillis = loadMillis;
                return this;
            }

            public Builder withComputeMillis(long computeMillis) {
                this.computeMillis = computeMillis;
                return this;
            }

            public Builder withWriteMillis(long writeMillis) {
                this.writeMillis = writeMillis;
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

            public Builder withProperty(String property) {
                this.property = property;
                return this;
            }

            public PageRankScore.Stats build() {
                return new PageRankScore.Stats(
                        nodes,
                        iterations,
                        loadMillis,
                        computeMillis,
                        writeMillis,
                        dampingFactor,
                        write,
                        property);
            }
        }
    }
}
