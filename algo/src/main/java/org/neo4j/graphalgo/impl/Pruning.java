package org.neo4j.graphalgo.impl;


import org.apache.commons.lang3.ArrayUtils;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.nd4j.linalg.indexing.NDArrayIndex.interval;


public class Pruning {

    private final double lambda;

    public Pruning() {
        this(0.7);
    }

    public Pruning(double lambda) {

        this.lambda = lambda;
    }

    public Embedding prune(Embedding prevEmbedding, Embedding embedding) {

        final Graph graph = loadFeaturesGraph(prevEmbedding, embedding);

//        int[] featureIdsToKeep = findConnectedComponents(graph)
//                .collect(Collectors.groupingBy(item -> item.setId))
//                .values()
//                .stream()
//                .mapToInt(results -> results.stream().mapToInt(value -> (int) value.nodeId).min().getAsInt())
//                .toArray();

        List<Integer> featureIdsToRemove = new ArrayList<>();
        Map<Long, List<DisjointSetStruct.Result>> bySetId = findConnectedComponents(graph).collect(Collectors.groupingBy(item -> item.setId));
        for (Long setId : bySetId.keySet()) {
            if(bySetId.get(setId).size() > 1) {
                int minId = bySetId.get(setId).stream().mapToInt(value -> (int) value.nodeId).min().getAsInt();

                for (DisjointSetStruct.Result result : bySetId.get(setId)) {
                    if(result.nodeId > minId) {
                        featureIdsToRemove.add((int) result.nodeId);
                    }
                }
            }
        }

        int nodeCount = prevEmbedding.getFeatures().length + embedding.getFeatures().length;
        int[] featureIdsToKeep = IntStream.range(0, nodeCount).filter(item -> !featureIdsToRemove.contains(item)).toArray();

        System.out.println("featureIdsToRemove = " + featureIdsToRemove);
        System.out.println("featureIdsToKeep = " + Arrays.toString(featureIdsToKeep));

        INDArray embeddingToPrune = Nd4j.hstack(prevEmbedding.getNDEmbedding(), embedding.getNDEmbedding());
        INDArray prunedNDEmbedding = pruneEmbedding(embeddingToPrune, featureIdsToKeep);

        Feature[][] featuresToPrune = ArrayUtils.addAll(prevEmbedding.getFeatures(), embedding.getFeatures());
        System.out.println("featuresToPrune = " + Arrays.deepToString(featuresToPrune));
        Feature[][] prunedFeatures = new Feature[featureIdsToKeep.length][];
        for (int index = 0; index < featureIdsToKeep.length; index++) {
            prunedFeatures[index] = featuresToPrune[featureIdsToKeep[index]];
        }

        return new Embedding(prunedFeatures, prunedNDEmbedding);
    }

    private Stream<DisjointSetStruct.Result> findConnectedComponents(Graph graph) {
        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute();
        algo.release();
        DSSResult dssResult = new DSSResult(struct);
        return dssResult.resultStream(graph);
    }

    private Graph loadFeaturesGraph(Embedding prevEmbedding, Embedding embedding) {
        int numPrevFeatures = prevEmbedding.getFeatures().length;
        int nodeCount = numPrevFeatures + embedding.getFeatures().length;
        IdMap idMap = new IdMap(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            idMap.add(i);
        }
        idMap.buildMappedIds();
        WeightMap relWeights = new WeightMap(nodeCount, 0, -1);
        AdjacencyMatrix matrix = new AdjacencyMatrix(idMap.size(), false);

        for (int prevFeatId = 0; prevFeatId < numPrevFeatures; prevFeatId++) {
            for (int featId = 0; featId < embedding.getFeatures().length; featId++) {
                INDArray emb1 = extractFeature(prevEmbedding.getNDEmbedding(), prevFeatId, 1);
                INDArray emb2 = extractFeature(embedding.getNDEmbedding(), featId, 1);

                double score = score(emb1, emb2);

                if (score > lambda) {
                    matrix.addOutgoing(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures));
                    relWeights.put(RawValues.combineIntInt(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures)), score);
                }
            }
        }

        return new HeavyGraph(idMap, matrix, relWeights, null, null);
    }

    private double[][] pruneEmbedding(double[][] origEmbedding, int... featIdsToKeep) {
        double[][] prunedEmbedding = new double[origEmbedding.length][];
        for (int i = 0; i < origEmbedding.length; i++) {
            prunedEmbedding[i] = new double[featIdsToKeep.length];
            for (int j = 0; j < featIdsToKeep.length; j++) {
                prunedEmbedding[i][j] = origEmbedding[i][featIdsToKeep[j]];
            }

        }
        return prunedEmbedding;
    }

    private INDArray pruneEmbedding(INDArray origEmbedding, int... featIdsToKeep) {
        INDArray ndPrunedEmbedding = Nd4j.create(origEmbedding.shape());
        Nd4j.copy(origEmbedding, ndPrunedEmbedding);
        return ndPrunedEmbedding.getColumns(featIdsToKeep);
    }


    private INDArray extractFeature(INDArray embedding, int id, int featureWidth) {

        return embedding.getColumn(id);

//        double[][] feature = new double[embedding.length][featureWidth];
//        for (int i = 0; i < embedding.length; i++) {
//            for (int w = 0; w < featureWidth; w++) {
//                feature[i][w] = embedding[i][id + w];
//            }
//        }
//        return feature;
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
        BOTH_DEGREE,
        DIFFUSE
    }

    static class Embedding {
        private final Feature[][] features;
        private INDArray ndEmbedding;

        public Embedding(Feature[][] features, INDArray ndEmbedding) {
            this.features = features;
            this.ndEmbedding = ndEmbedding;
        }

        public Feature[][] getFeatures() {
            return features;
        }

        public int numFeatures() {
            return this.getFeatures().length;
        }

        public INDArray getNDEmbedding() {
            return ndEmbedding;
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


    double score(INDArray feat1, INDArray feat2) {

        return feat1.eq(feat2).sum(0).getDouble(0,0) / feat1.size(0);
//        int match = 0;
//        for (int i = 0; i < feat1.length; i++) {
//            if (Arrays.equals(feat1[i], feat2[i])) {
//                match++;
//            }
//        }
//        return (double) match / feat1.length;

//        return 0;
    }


}
