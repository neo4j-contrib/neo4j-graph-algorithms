package org.neo4j.graphalgo.impl;

import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.impl.Pruning.Feature.*;

public class PruningTest {

    @Test
    public void singleColumnFeatures() {
        double[][] one = {
                {1},
                {2},
                {3}
        };

        double[][] two = {
                {1},
                {2},
                {3}
        };

        double[][] three = {
                {2},
                {2},
                {3}
        };


        Pruning pruning = new Pruning();
        assertEquals(1.0, pruning.score(one, two), 0.01);
        assertEquals(0.66, pruning.score(one, three), 0.01);
    }

    @Test
    public void multiColumnFeatures() {
        double[][] one = {
                {1,2,3},
                {2,3,4},
                {3,4,5}
        };

        double[][] two = {
                {1,2,4},
                {2,3,4},
                {3,1,5}
        };

        double[][] three = {
                {1,2,3},
                {2,3,4},
                {5,4,3}
        };


        Pruning pruning = new Pruning();
        assertEquals(0.33, pruning.score(one, two), 0.01);
        assertEquals(0.66, pruning.score(one, three), 0.01);
    }

    @Test
    public void pruning() {
        // in, out, both

        double[][] one = {
                {1,2,3},
                {2,3,4},
                {3,4,5}
        };

        // mean-in, mean-out, mean-both, other

        double[][] two = {
                {1,2,4,3},
                {2,3,4,3},
                {3,1,5,4}
        };

        Pruning pruning = new Pruning();

        Pruning.Embedding prevEmbedding = new Pruning.Embedding(new Pruning.Feature[][]{{IN}, {OUT}, {BOTH}}, one);
        Pruning.Embedding embedding = new Pruning.Embedding(new Pruning.Feature[][]{{MEAN, IN},{MEAN, OUT},{MEAN, BOTH},{OTHER}}, two);

        Pruning.Embedding prunedEmbedding = pruning.prune(prevEmbedding, embedding);
    }

    class Edge {
        private final Pruning.Feature[][] feat1;
        private final Pruning.Feature[][] feat2;
        private final double similarity;

        Edge(Pruning.Feature[][] feat1, Pruning.Feature[][] feat2, double similarity){

            this.feat1 = feat1;
            this.feat2 = feat2;
            this.similarity = similarity;
        }
    }

    @Test
    public void connectedComponents() {

//        Edge[] graph = {
//                new Edge(new Pruning.Feature[][] { {IN} }, new Pruning.Feature[][] { {MEAN, IN} }, 1.0)
//        };

//        calculateConnectedComponents(graph);

        IdMap idMap= new IdMap(10);

        idMap.add(0);
        idMap.add(2);
        idMap.add(4);

        idMap.add(10);
        idMap.add(12);
        idMap.add(14);
        idMap.add(16);

        idMap.buildMappedIds();

        AdjacencyMatrix matrix = new AdjacencyMatrix(idMap.size(), false);
        matrix.addOutgoing(idMap.get(0), idMap.get(10));
        matrix.addOutgoing(idMap.get(2), idMap.get(12));
        matrix.addOutgoing(idMap.get(2), idMap.get(16));
        matrix.addOutgoing(idMap.get(4), idMap.get(14));

        WeightMapping relWeights = new WeightMap(10, 0, -1);
        relWeights.set(RawValues.combineIntInt(idMap.get(0), idMap.get(10)), 1.0);
        relWeights.set(RawValues.combineIntInt(idMap.get(2), idMap.get(12)), 0.66);
        relWeights.set(RawValues.combineIntInt(idMap.get(2), idMap.get(14)), 0.66);
        relWeights.set(RawValues.combineIntInt(idMap.get(4), idMap.get(14)), 0.66);

        final Graph graph = new HeavyGraph(idMap, matrix, relWeights, null, null);

        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute();
        algo.release();
        DSSResult dssResult = new DSSResult(struct);
        Stream<DisjointSetStruct.Result> resultStream = dssResult.resultStream(graph);

        resultStream.forEach(item -> {
            System.out.println(item.nodeId + " -> " + item.setId);
        });

    }

    @Test
    public void unionFindEmbeddings() {
        double[][] one = {
                {1,2,3},
                {2,3,4},
                {3,4,5}
        };

        // mean-in, mean-out, mean-both, other

        double[][] two = {
                {1,2,4,3},
                {2,3,4,3},
                {3,1,5,4}
        };

        Pruning pruning = new Pruning();

        Pruning.Embedding prevEmbedding = new Pruning.Embedding(new Pruning.Feature[][]{{IN}, {OUT}, {BOTH}}, one);
        Pruning.Embedding embedding = new Pruning.Embedding(new Pruning.Feature[][]{{MEAN, IN},{MEAN, OUT},{MEAN, BOTH},{OTHER}}, two);

        int numPrevFeatures = prevEmbedding.getFeatures().length;
        int nodeCount = numPrevFeatures + embedding.getFeatures().length;
        IdMap idMap= new IdMap(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            idMap.add(i);
        }
        idMap.buildMappedIds();
        WeightMapping relWeights = new WeightMap(nodeCount, 0, -1);
        AdjacencyMatrix matrix = new AdjacencyMatrix(idMap.size(), false);

        for (int prevFeatId = 0; prevFeatId < numPrevFeatures; prevFeatId++) {
            for (int featId = 0; featId < embedding.getFeatures().length; featId++) {
                double[][] emb1 = getFeature(prevEmbedding.getEmbedding(), prevFeatId, 1);
                double[][] emb2 = getFeature(embedding.getEmbedding(), featId, 1);

                double score = pruning.score(emb1, emb2);

                if(score > 0.5) {
                    matrix.addOutgoing(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures));
                    relWeights.set(RawValues.combineIntInt(idMap.get(prevFeatId), idMap.get(featId + numPrevFeatures)), score);
                    System.out.println("pruning.score(emb1,emb2) = " + Arrays.deepToString(emb1) + " " + Arrays.deepToString(emb2) + " " + score);

                }

            }
        }

        final Graph graph = new HeavyGraph(idMap, matrix, relWeights, null, null);

        GraphUnionFind algo = new GraphUnionFind(graph);
        DisjointSetStruct struct = algo.compute();
        algo.release();
        DSSResult dssResult = new DSSResult(struct);
        Stream<DisjointSetStruct.Result> resultStream = dssResult.resultStream(graph);

        resultStream.forEach(item -> {
            System.out.println(item.nodeId + " -> " + item.setId);
        });
    }

    double[][] getFeature(double[][] embedding, int id, int featureWidth) {

        double[][] feature = new double[embedding.length][featureWidth];
        for (int i = 0; i < embedding.length; i++) {
            for (int w = 0; w < featureWidth; w++) {
                feature[i][w] = embedding[i][id + w];
            }
        }
        return feature;
    }

    @Test
    public void testGetFeature() {
        double[][] one = {
                {1,2,3},
                {2,3,4},
                {3,4,5}
        };

        // mean-in, mean-out, mean-both, other

        double[][] two = {
                {1,2,4,3},
                {2,3,4,3},
                {3,1,5,4}
        };

        double[][] feature = getFeature(two, 2, 2);
        System.out.println("feature = " + Arrays.deepToString(feature));
    }
}