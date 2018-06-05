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

public class Nd4jSandboxTest {
    @Test
    public void shouldSliceArray() throws Exception {
        // given

        INDArray embedding = Nd4j.create(new double[][]{
                {1, 2, 3},
                {4, 5, 6},
                {7, 8, 9}
        });

        INDArray columns = embedding.getColumns(1, 2);

        System.out.println("columns = " + columns);
    }

    @Test
    public void shouldConcatArrays() throws Exception {
        // given
        INDArray one = Nd4j.create(new double[][]{
                {1, 2, 3},
                {4, 5, 6},
                {7, 8, 9}
        });

        INDArray two = Nd4j.create(new double[][]{
                {10, 11, 12},
                {13, 14, 15},
                {16, 17, 18}
        });

        INDArray concat = Nd4j.concat(1, one, two);

        System.out.println("concat = " + concat);

        INDArray hstack = Nd4j.hstack(one, two);

        System.out.println("hstack = " + hstack);


        // when

        // then
    }


}
