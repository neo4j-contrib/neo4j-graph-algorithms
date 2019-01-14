package org.neo4j.graphalgo.similarity;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class WeightedInputTest {

    @Test
    public void pearsonNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.pearson(null, -1.0, input2);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.pearson(decoder, -1.0, input2);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.pearsonSkip(null, -1.0, input2, 0.0);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.pearsonSkip(decoder, -1.0, input2, 0.0);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.cosineSquares(null, -1.0, input2);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.cosineSquares(decoder, -1.0, input2);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.cosineSquaresSkip(null, -1.0, input2, 0.0);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.cosineSquaresSkip(decoder, -1.0, input2, 0.0);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.sumSquareDelta(null, -1.0, input2);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.sumSquareDelta(decoder, -1.0, input2);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.sumSquareDeltaSkip(null, -1.0, input2, 0.0);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.sumSquareDeltaSkip(decoder, -1.0, input2, 0.0);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }


}