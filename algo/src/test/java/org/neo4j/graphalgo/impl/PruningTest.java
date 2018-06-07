package org.neo4j.graphalgo.impl;

import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.heavyweight.AdjacencyMatrix;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PruningTest {

    @Test
    public void testGetCols() {

        INDArray origEmbedding = Nd4j.create(new double[][]{
                {0.00, 1.00, 0.00},
                {0.00, 0.00, 1.00},
                {0.00, 1.00, 1.00},
                {0.00, 2.00, 2.00},
                {1.00, 0.00, 0.00},
                {1.00, 0.00, 0.00},
                {2.00, 0.00, 0.00},
        });

        int[] featIdsToKeep = {2, 1, 0};

        INDArray ndPrunedEmbedding = Nd4j.create(origEmbedding.shape());
        Nd4j.copy(origEmbedding, ndPrunedEmbedding);
        INDArray columns = ndPrunedEmbedding.getColumns(featIdsToKeep);
        System.out.println("columns = \n" + columns);
    }

    @Test
    public void singleColumnFeatures() {
        INDArray one = Nd4j.create(new double[][]{
                {1},
                {2},
                {3}
        });

        INDArray two = Nd4j.create(new double[][]{
                {1},
                {2},
                {3}
        });

        INDArray three = Nd4j.create(new double[][]{
                {2},
                {2},
                {3}
        });


        Pruning pruning = new Pruning(0.1);
        assertEquals(1.0, pruning.score(one, two), 0.01);
        assertEquals(0.66, pruning.score(one, three), 0.01);
    }

    @Test
    public void pruning() {
        // in, out, both

        double[][] one = {
                {1, 2, 3},
                {2, 3, 4},
                {3, 4, 5}
        };

        // mean-in, mean-out, mean-both, other

        double[][] two = {
                {1, 2, 4, 3},
                {2, 3, 4, 3},
                {3, 1, 5, 4}
        };

        Pruning pruning = new Pruning();

        Pruning.Embedding prevEmbedding = new Pruning.Embedding(new Pruning.Feature[]{
                new Pruning.Feature("IN_DEGREE"),
                new Pruning.Feature("OUT_DEGREE"),
                new Pruning.Feature("BOTH_DEGREE"),
        }, Nd4j.create(one));
        Pruning.Embedding embedding = new Pruning.Embedding(new Pruning.Feature[]{
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("IN_DEGREE")),
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("OUT_DEGREE")),
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("BOTH_DEGREE")),
                new Pruning.Feature("MEAN_BOTH_NEIGHBOURHOOD"),
                }, Nd4j.create(two));

        Pruning.Embedding prunedEmbedding = pruning.prune(prevEmbedding, embedding);
    }

    @Test
    public void connectedComponents() {
        IdMap idMap = new IdMap(10);

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

        WeightMap relWeights = new WeightMap(10, 0, -1);
        relWeights.put(RawValues.combineIntInt(idMap.get(0), idMap.get(10)), 1.0);
        relWeights.put(RawValues.combineIntInt(idMap.get(2), idMap.get(12)), 0.66);
        relWeights.put(RawValues.combineIntInt(idMap.get(2), idMap.get(14)), 0.66);
        relWeights.put(RawValues.combineIntInt(idMap.get(4), idMap.get(14)), 0.66);

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
    public void pruneEmbeddingsAndFeatures() {
        double[][] prevLayer = {
                {1, 2, 3},
                {2, 3, 4},
                {3, 4, 5}
        };

        Pruning.Feature[] prevLayerFeatures = new Pruning.Feature[]{
                new Pruning.Feature("IN_DEGREE"),
                new Pruning.Feature("OUT_DEGREE"),
                new Pruning.Feature("BOTH_DEGREE"),
        };

        double[][] layer = {
                {1, 2, 3, 9},
                {2, 3, 4, 9},
                {3, 4, 5, 8}
        };

        Pruning.Feature[] layerFeatures = new Pruning.Feature[]{
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("IN_DEGREE")),
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("OUT_DEGREE")),
                new Pruning.Feature("MEAN_IN_NEIGHBOURHOOD", new Pruning.Feature("BOTH_DEGREE")),
                new Pruning.Feature("MEAN_BOTH_NEIGHBOURHOOD"),
        };

        Pruning pruning = new Pruning(0.5);

        // make sure that we prune away the complex features
        // i.e. we should keep the feature from prevEmbedding wherever possible
        Pruning.Embedding prevEmbedding = new Pruning.Embedding(prevLayerFeatures, Nd4j.create(prevLayer));
        Pruning.Embedding embedding = new Pruning.Embedding(layerFeatures, Nd4j.create(layer));
        Pruning.Embedding prunedEmbedding = pruning.prune(prevEmbedding, embedding);

        System.out.println("Embedding:");
        System.out.println(prunedEmbedding.getNDEmbedding());

        assertEquals(Nd4j.create(new double[][]{
                {1, 2, 3, 9},
                {2, 3, 4, 9},
                {3, 4, 5, 8},
        }), prunedEmbedding.getNDEmbedding());

        System.out.println("Features:");
        System.out.println(Arrays.deepToString(prunedEmbedding.getFeatures()));

        Pruning.Feature[] expected = new Pruning.Feature[]{
                new Pruning.Feature("IN_DEGREE"),
                new Pruning.Feature("OUT_DEGREE"),
                new Pruning.Feature("BOTH_DEGREE"),
                new Pruning.Feature("MEAN_OUT_NEIGHBOURHOOD"),
        };

        assertArrayEquals(expected, prunedEmbedding.getFeatures());
    }

    @Test
    public void shouldOnlyKeepFeaturesThatAddSomethingUnique() {
        double[][] prevLayer = {
                {1, 2, 3},
                {2, 3, 4},
                {3, 4, 5}
        };

        Pruning.Feature[] prevLayerFeatures = new Pruning.Feature[]{
                new Pruning.Feature("IN_DEGREE"),
                new Pruning.Feature("OUT_DEGREE"),
                new Pruning.Feature("BOTH_DEGREE"),
        };

        double[][] layer = {
                {1, 2, 3, 9},
                {2, 3, 4, 9},
                {3, 4, 5, 8}
        };

        Pruning.Feature[] layerFeatures = new Pruning.Feature[]{
                new Pruning.Feature("MEAN_BOTH_NEIGHBOURHOOD", new Pruning.Feature("IN_DEGREE")),
                new Pruning.Feature("MEAN_BOTH_NEIGHBOURHOOD", new Pruning.Feature("OUT_DEGREE")),
                new Pruning.Feature("MEAN_BOTH_NEIGHBOURHOOD", new Pruning.Feature("BOTH_DEGREE")),
                new Pruning.Feature("MEAN_OUT_NEIGHBOURHOOD"),
        };

        Pruning pruning = new Pruning(0.5);

        // make sure that we prune away the complex features
        // i.e. we should keep the feature from prevEmbedding wherever possible
        Pruning.Embedding prevEmbedding = new Pruning.Embedding(prevLayerFeatures, Nd4j.create(prevLayer));
        Pruning.Embedding embedding = new Pruning.Embedding(layerFeatures, Nd4j.create(layer));
        Pruning.Embedding prunedEmbedding = pruning.prune(prevEmbedding, embedding);

        assertEquals(Nd4j.create(new double[][]{
                {9},
                {9},
                {8},
        }), prunedEmbedding.getNDEmbedding());

        System.out.println("Features:");
        System.out.println(Arrays.deepToString(prunedEmbedding.getFeatures()));

        Pruning.Feature[] expected = new Pruning.Feature[] {
                new Pruning.Feature("MEAN_OUT_NEIGHBOURHOOD")
        };

        assertArrayEquals(expected, prunedEmbedding.getFeatures());

    }

}
