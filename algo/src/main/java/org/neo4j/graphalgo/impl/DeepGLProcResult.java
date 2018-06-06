package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

public class DeepGLProcResult {
    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final long embeddingSize;
//    public final List<List<String>> features;

    private DeepGLProcResult(long loadMillis, long computeMillis, long writeMillis, long nodes, int embeddingSize, String writeProperty) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.embeddingSize = embeddingSize;
    }

    public static DeepGLProcResult.Builder builder() {
        return new DeepGLProcResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<DeepGLProcResult> {

        private long nodes = 0;
        private int embeddingSize = 0;
        private String writeProperty = "";

        public DeepGLProcResult.Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public DeepGLProcResult build() {
            return new DeepGLProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    embeddingSize,
                    writeProperty);
        }

        public DeepGLProcResult.Builder withEmbeddingSize(int embeddingSize) {
            this.embeddingSize = embeddingSize;
            return this;
        }

        public Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }
    }
}
