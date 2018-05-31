package org.neo4j.graphalgo.impl;


import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

public class Binning {

    public void linearBins(double[][] embedding, int numBins) {


        INDArray indArray = Nd4j.create(embedding);
        for (int column = 0; column < embedding[0].length; column++) {
            INDArray slice = indArray.slice(column, 1);
            INDArray[] indArrays = Nd4j.sortWithIndices(slice, 0, true);
            INDArray indices = indArrays[0];
            int maxRank = embedding.length;
            for (int rank = 0; rank < indices.size(0); rank++) {
                embedding[(int) indices.getDouble(rank)][column] = (int) (((double) rank / maxRank) * numBins);

            }
        }
    }

    public void logBins(double[][] embedding) {
        INDArray indArray = Nd4j.create(embedding);
        for (int column = 0; column < embedding[0].length; column++) {
            int n = embedding.length;
            int remaining = n;
            int binNumber = 0;
            INDArray slice = indArray.slice(column, 1);
            INDArray[] indArrays = Nd4j.sortWithIndices(slice, 0, true);
            INDArray indices = indArrays[0];

            for (int node = 0; node < embedding.length; node++) {
                if (node + remaining == embedding.length) {
                    remaining /= 2;
                    binNumber++;
                }
                embedding[(int) indices.getDouble(node)][column] = binNumber - 1;
            }
        }
    }
}
