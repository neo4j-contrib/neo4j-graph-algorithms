package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

class WeightedInput implements Comparable<WeightedInput> {
    private final long id;
    private int itemCount;
    private final double[] weights;
    final int initialSize;

    public WeightedInput(long id, double[] weights, int fullSize, int itemCount) {
        this.initialSize = fullSize;
        this.id = id;
        this.weights = weights;
        this.itemCount = itemCount;
    }

    public WeightedInput(long id, double[] weights, double skipValue) {
        this(id, weights, weights.length, calculateCount(weights, skipValue));
    }

    public WeightedInput(long id, double[] weights) {
        this(id, weights, weights.length, weights.length);
    }

    private static int calculateCount(double[] weights, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);
        int count =0;
        for (double weight : weights) {
            if (!(weight == skipValue || (skipNan && Double.isNaN(weight)))) count++;
        }
        return count;
    }

    public static WeightedInput sparse(long id, double[] weights, int fullSize, int compressedSize) {
        return new WeightedInput(id, weights, fullSize, compressedSize);
    }

    public static WeightedInput dense(long id, double[] weights, double skipValue) {
        return new WeightedInput(id, weights, skipValue);
    }

    public static WeightedInput dense(long id, double[] weights) {
        return new WeightedInput(id, weights);
    }

    public int compareTo(WeightedInput o) {
        return Long.compare(id, o.id);
    }

    public SimilarityResult sumSquareDeltaSkip(double similarityCutoff, WeightedInput other, double skipValue) {
        int len = Math.min(weights.length, other.weights.length);
        double sumSquareDelta = Intersections.sumSquareDeltaSkip(weights, other.weights, len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, sumSquareDelta);
    }

    public SimilarityResult sumSquareDelta(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double sumSquareDelta = Intersections.sumSquareDelta(weights, other.weights, len);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, sumSquareDelta);
    }

    public SimilarityResult cosineSquaresSkip(RleDecoder decoder, double similarityCutoff, WeightedInput other, double skipValue) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if(decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquareSkip(thisWeights, otherWeights, len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, cosineSquares);
    }

    public SimilarityResult cosineSquares(RleDecoder decoder, double similarityCutoff, WeightedInput other) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if(decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquare(thisWeights, otherWeights, len);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, cosineSquares);
    }

    public SimilarityResult pearsonSquares(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double pearsonSquares = Intersections.pearsonSquare(weights, other.weights, len);
        long intersection = 0;
        if (similarityCutoff >= 0d && (pearsonSquares == 0 || pearsonSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, pearsonSquares);
    }

    public SimilarityResult pearsonSquaresSkip(double similarityCutoff, WeightedInput other, Double skipValue) {
        int len = Math.min(weights.length, other.weights.length);
        double cosineSquares = Intersections.pearsonSquareSkip(weights, other.weights, len, skipValue);
        long intersection = 0;
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }
}
