package org.neo4j.graphalgo.impl;


import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.cpu.nativecpu.NDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.nd4j.linalg.indexing.NDArrayIndex.interval;


public class Pruning {

    private final double lambda;

    public Pruning() {
        this(0.1);
    }
    public Pruning(double lambda) {

        this.lambda = lambda;
    }

    public Embedding prune(Embedding prevEmbedding, Embedding embedding) {

        final Graph graph = loadFeaturesGraph(prevEmbedding, embedding);
        Stream<DisjointSetStruct.Result> resultStream = findConnectedComponents(graph);

        int[] featureIdsToKeep = resultStream
                .filter(item -> item.nodeId >= prevEmbedding.numFeatures())
                .collect(Collectors.groupingBy(item -> item.setId))
                .values()
                .stream()
                .mapToInt(results -> (int) (results.stream().findFirst().get().nodeId - prevEmbedding.numFeatures()))
                .toArray();

        double[][] prunedEmbedding = pruneEmbedding(embedding.getEmbedding(), featureIdsToKeep);
        INDArray prunedNDEmbedding = pruneEmbedding(embedding.getNDEmbedding(), featureIdsToKeep);

        Feature[][] prunedFeatures = Arrays.stream(featureIdsToKeep).mapToObj(i -> embedding.getFeatures()[(int) i]).toArray(Feature[][]::new);

        return new Embedding(prunedFeatures, prunedEmbedding, prunedNDEmbedding);
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
                double[][] emb1 = extractFeature(prevEmbedding.getEmbedding(), prevFeatId, 1);
                double[][] emb2 = extractFeature(embedding.getEmbedding(), featId, 1);

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
        BOTH_DEGREE,
        DIFFUSE
    }

    static class Embedding {
        private final Feature[][] features;
        private final double[][] embedding;
        private INDArray ndEmbedding;

        public Embedding(Feature[][] features, double[][] embedding, INDArray ndEmbedding) {
            this.features = features;
            this.embedding = embedding;
            this.ndEmbedding = ndEmbedding;
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


}
