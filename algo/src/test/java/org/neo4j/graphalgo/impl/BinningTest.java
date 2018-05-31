package org.neo4j.graphalgo.impl;

import org.junit.Test;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.Arrays;

public class BinningTest {

    int numBins(int length, double alpha) {

        return (int) Math.floor(Math.log(length) / Math.log(1 / alpha));
    }

    @Test
    public void linearBins() {
        double[][] embedding = new double[][]{
                {1, 2, 3},
                {4, 5, 6},
                {3, 1, 5},
                {3, 1, 2},
                {1, 2, 1},
                {4, 5, 8},
                {3, 1, 20},
                {3, 1, 5},
        };

        Binning binning = new Binning();
        binning.linearBins(embedding, 3);
        System.out.println("embedding = " + Arrays.deepToString(embedding));
    }

    @Test
    public void testNumBins() {
        System.out.println(numBins(16, 0.5));

    }

    @Test
    public void testLogBins() {

        double[][] embedding = new double[][]{
                {1, 2, 3},
                {4, 5, 6},
                {3, 1, 5},
                {3, 1, 0},
                {1, 2, 1},
                {4, 5, 8},
                {3, 1, 20},
                {3, 1, 5},
        };


        INDArray indArray = Nd4j.create(embedding);
        System.out.println("embedding = \n" + indArray);

        Binning binning = new Binning();
        binning.logBins(embedding);

        System.out.println("embedding = \n" + Nd4j.create(embedding));
    }
}
