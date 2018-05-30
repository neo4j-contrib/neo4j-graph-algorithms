package org.neo4j.graphalgo.impl;


import java.util.Arrays;

public class Pruning {

    public enum Feature implements F {
        IN_DEGREE,
        OUT_DEGREE,
        BOTH_DEGREE,
        MEAN_IN_NEIGHBOURHOOD,
        MEAN_OUT_NEIGHBOURHOOD,
        MEAN_BOTH_NEIGHOURHOOD;


        @Override
        public int multiplier() {
            return 1;
        }
    }

    interface F{
        int multiplier();
    }

    static class Embedding {
        private final Feature[][] features;
        private final double[][] embedding;

        public Embedding(Feature[][] features, double[][] embedding) {

            this.features = features;
            this.embedding = embedding;
        }

        public Feature[][] getFeatures() {
            return features;
        }

        public double[][] getEmbedding() {
            return embedding;
        }

        public int numFeatures() {
            return this.getFeatures().length;
        }
    }

    public Embedding prune(Embedding prevEmbedding, Embedding embedding) {

        return prevEmbedding;
    }

    double score(double[][] feat1, double[][] feat2) {
        int match = 0;
        for (int i = 0; i < feat1.length; i++) {
            if (Arrays.equals(feat1[i], feat2[i])) {
                match++;
            }
        }
        return (double) match / feat1.length;
    }


}
