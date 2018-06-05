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


        Pruning pruning = new Pruning(0.1);
        assertEquals(1.0, pruning.score(one, two), 0.01);
        assertEquals(0.66, pruning.score(one, three), 0.01);
    }

    @Test
    public void multiColumnFeatures() {
        double[][] one = {
                {1, 2, 3},
                {2, 3, 4},
                {3, 4, 5}
        };

        double[][] two = {
                {1, 2, 4},
                {2, 3, 4},
                {3, 1, 5}
        };

        double[][] three = {
                {1, 2, 3},
                {2, 3, 4},
                {5, 4, 3}
        };


        Pruning pruning = new Pruning();
        assertEquals(0.33, pruning.score(one, two), 0.01);
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

        Pruning.Embedding prevEmbedding = new Pruning.Embedding(new Pruning.Feature[][]{{IN_DEGREE}, {OUT_DEGREE}, {BOTH_DEGREE}}, one, Nd4j.create(one));
        Pruning.Embedding embedding = new Pruning.Embedding(new Pruning.Feature[][]{{MEAN_IN_NEIGHBOURHOOD, IN_DEGREE}, {MEAN_IN_NEIGHBOURHOOD, OUT_DEGREE}, {MEAN_IN_NEIGHBOURHOOD, BOTH_DEGREE}, {MEAN_BOTH_NEIGHOURHOOD}}, two, Nd4j.create(two));

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
    public void unionFindEmbeddings() {
        double[][] prevLayer = {
                {1, 2, 3},
                {2, 3, 4},
                {3, 4, 5}
        };

        // mean-in, mean-out, mean-both, other

        double[][] layer = {
                {1, 2, 4, 3},
                {2, 3, 4, 3},
                {3, 1, 5, 4}
        };

        Pruning pruning = new Pruning();

        Pruning.Embedding prevEmbedding = new Pruning.Embedding(new Pruning.Feature[][]{{IN_DEGREE}, {OUT_DEGREE}, {BOTH_DEGREE}}, prevLayer, Nd4j.create(prevLayer));
        Pruning.Embedding embedding = new Pruning.Embedding(new Pruning.Feature[][]{{MEAN_BOTH_NEIGHOURHOOD, IN_DEGREE}, {MEAN_BOTH_NEIGHOURHOOD, OUT_DEGREE}, {MEAN_BOTH_NEIGHOURHOOD, BOTH_DEGREE}, {MEAN_OUT_NEIGHBOURHOOD}}, layer, Nd4j.create(layer));


        // make sure that we prune away the complex features
        // i.e. we should keep the feature from prevEmbedding wherever possible

        Pruning.Embedding prunedEmbedding = pruning.prune(prevEmbedding, embedding);
        System.out.println("Embedding:");
        System.out.println(Arrays.deepToString(prunedEmbedding.getEmbedding()));
        System.out.println("Features:");
        System.out.println(Arrays.deepToString(prunedEmbedding.getFeatures()));
    }
}
