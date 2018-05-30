package org.neo4j.graphalgo.impl;


import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Pruning {

    public Embedding prune(Embedding prevEmbedding, Embedding embedding) {

        int numPrevFeatures = prevEmbedding.getFeatures().length;
        int nodeCount = numPrevFeatures + embedding.getFeatures().length;
        IdMap idMap = new IdMap(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            idMap.add(i);
        }
        idMap.buildMappedIds();
        WeightMapping relWeights = new WeightMap(nodeCount, 0, -1);
        AdjacencyMatrix matrix = new AdjacencyMatrix(idMap.size(), false);

        for (int prevFeatId = 0; prevFeatId < numPrevFeatures; prevFeatId++) {
            for (int featId = 0; featId < embedding.getFeatures().length; featId++) {
                double[][] emb1 = extractFeature(prevEmbedding.getEmbedding(), prevFeatId, 1);
                double[][] emb2 = extractFeature(embedding.getEmbedding(), featId, 1);

                double score = score(emb1, emb2);

                if (score > 0.5) {
                    matrix.addOutgoing(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures));
                    relWeights.set(RawValues.combineIntInt(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures)), score);

                }

            }
        }

        final Graph graph = new HeavyGraph(idMap, matrix, relWeights, null, null);

        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute();
        algo.release();
        DSSResult dssResult = new DSSResult(struct);
        Stream<DisjointSetStruct.Result> resultStream = dssResult.resultStream(graph);

        long[] featureIdsToKeep = resultStream
                .filter(item -> item.nodeId >= prevEmbedding.numFeatures())
                .collect(Collectors.groupingBy(item -> item.setId))
                .values()
                .stream()
                .mapToLong(results -> results.stream().findFirst().get().nodeId - prevEmbedding.numFeatures())
                .toArray();

        double[][] prunedEmbedding = pruneEmbedding(embedding.getEmbedding(), featureIdsToKeep);
        Feature[][] prunedFeatures = Arrays.stream(featureIdsToKeep).mapToObj(i -> embedding.getFeatures()[(int) i]).toArray(Feature[][]::new);

        return new Embedding(prunedFeatures, prunedEmbedding);
    }

    private double[][] pruneEmbedding(double[][] origEmbedding, long... featIdsToKeep) {
        double[][] prunedEmbedding = new double[origEmbedding.length][];
        for (int i = 0; i < origEmbedding.length; i++) {
            prunedEmbedding[i] = new double[featIdsToKeep.length];
            for (int j = 0; j < featIdsToKeep.length; j++) {
                prunedEmbedding[i][j] = origEmbedding[i][(int) featIdsToKeep[j]];
            }

        }
        return prunedEmbedding;
    }

    private double[][] extractFeature(double[][] embedding, int id, int featureWidth) {

        double[][] feature = new double[embedding.length][featureWidth];
        for (int i = 0; i < embedding.length; i++) {
            for (int w = 0; w < featureWidth; w++) {
                feature[i][w] = embedding[i][id + w];
            }
        }
        return feature;
    }

    public enum Feature {
        SUM_OUT_NEIGHBOURHOOD,
        SUM_IN_NEIGHBOURHOOD,
        SUM_BOTH_NEIGHOURHOOD,
        HADAMARD_OUT_NEIGHBOURHOOD,
        HADAMARD_IN_NEIGHBOURHOOD,
        HADAMARD_BOTH_NEIGHOURHOOD,
        MAX_OUT_NEIGHBOURHOOD,
        MAX_IN_NEIGHBOURHOOD,
        MAX_BOTH_NEIGHOURHOOD,
        MEAN_OUT_NEIGHBOURHOOD,
        MEAN_IN_NEIGHBOURHOOD,
        MEAN_BOTH_NEIGHOURHOOD,
        RBF_OUT_NEIGHBOURHOOD,
        RBF_IN_NEIGHBOURHOOD,
        RBF_BOTH_NEIGHOURHOOD,
        L1NORM_OUT_NEIGHBOURHOOD,
        L1NORM_IN_NEIGHBOURHOOD,
        L1NORM_BOTH_NEIGHOURHOOD,
        OUT_DEGREE,
        IN_DEGREE,
        BOTH_DEGREE
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
