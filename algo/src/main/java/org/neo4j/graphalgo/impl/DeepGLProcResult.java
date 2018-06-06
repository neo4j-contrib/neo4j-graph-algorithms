package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DeepGLProcResult {
    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long nodes;
    public final String writeProperty;
    public final long embeddingSize;
    public final long numberOfLayers;
    public final List<List<String>> features;

    private DeepGLProcResult(long loadMillis, long computeMillis, long writeMillis, long nodes, String writeProperty, int embeddingSize, List<List<String>> features, int numberOfLayers) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.embeddingSize = embeddingSize;
        this.writeProperty = writeProperty;
        this.features = features;
        this.numberOfLayers = numberOfLayers;
    }

    public static DeepGLProcResult.Builder builder() {
        return new DeepGLProcResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<DeepGLProcResult> {

        private long nodes = 0;
        private int embeddingSize = 0;
        private String writeProperty = "";
        private List<List<String>> features = new ArrayList<>();
        private int numberOfLayers;

        public DeepGLProcResult.Builder withNodeCount(long nodes) {
            this.nodes = nodes;
            return this;
        }

        public DeepGLProcResult build() {
            return new DeepGLProcResult(loadDuration,
                    evalDuration,
                    writeDuration,
                    nodes,
                    writeProperty,
                    embeddingSize,
                    features,
                    numberOfLayers);
        }

        public DeepGLProcResult.Builder withEmbeddingSize(int embeddingSize) {
            this.embeddingSize = embeddingSize;
            return this;
        }

        public DeepGLProcResult.Builder withWriteProperty(String writeProperty) {
            this.writeProperty = writeProperty;
            return this;
        }

        public DeepGLProcResult.Builder withFeatures(Pruning.Feature[][] features) {
            for (Pruning.Feature[] feature : features) {
                this.features.add(Arrays.stream(feature).map(Enum::name).collect(Collectors.toList()));
            }

            return this;
        }

        public DeepGLProcResult.Builder withLayers(int numberOfLayers) {
            this.numberOfLayers = numberOfLayers;
            return this;
        }
    }
}
