package org.neo4j.graphalgo.impl;

import org.junit.Test;

import java.util.Arrays;

public class BinningTest {

    int numBins(int length, double alpha) {

        return (int) Math.floor(Math.log(length) / Math.log(1 / alpha)) + 1;
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
        binning.linearBins(embedding);
        System.out.println("embedding = " + Arrays.deepToString(embedding));
    }

    @Test
    public void testNumBins() {
        System.out.println(numBins(16, 0.5));

    }
}