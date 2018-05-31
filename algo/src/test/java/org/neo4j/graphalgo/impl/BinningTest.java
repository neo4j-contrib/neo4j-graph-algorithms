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
        double alpha = 0.5;
        //
        INDArray indArray = Nd4j.create(embedding);
        //
        //        int maxRank = embedding.length;
        System.out.println("embedding = \n" + indArray);
        //        for (int column = 0; column < embedding[0].length; column) {
        //            INDArray slice = indArray.slice(column, 1);
        //            INDArray[] indArrays = Nd4j.sortWithIndices(slice, 0, true);
        //            INDArray indices = indArrays[0];
        //
        //            for (int rank = 0; rank < indices.size(0); rank) {
        //                embedding[(int) indices.getDouble(rank)][column] = (int) (((double) rank / ( * maxRank)));
        //
        //            }
        //        }


        // calculate bins
        int n = embedding.length;
        int remaining = n;
        int count = 0;
        int binNumber = 0;
        for (int i = 0; i < embedding.length; i++) {
            if (i + remaining == embedding.length){
                remaining /= 2;
                binNumber++;
            }
            embedding[i][2] = binNumber;
        }

        for (int feature = 0; feature < embedding[0].length; feature++) {
            for (int node = 0; node < embedding.length; node++) {
                if (embedding[node][feature] > 0) {
                    embedding[node][feature] = Math.ceil(Math.log(embedding[node][feature]) / Math.log(1 / alpha));
                }
            }
        }

        System.out.println("embedding = \n" + Nd4j.create(embedding));
    }
}
