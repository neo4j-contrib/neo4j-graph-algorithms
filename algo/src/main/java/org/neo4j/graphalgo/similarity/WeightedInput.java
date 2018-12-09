package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

class WeightedInput implements Comparable<WeightedInput> {
    long id;
    double[] weights;
    int count;

    public WeightedInput(long id, double[] weights, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);
        this.id = id;
        this.weights = weights;
        for (double weight : weights) {
            if (!(weight == skipValue || (skipNan && Double.isNaN(weight)))) this.count++;
        }
    }

    public WeightedInput(long id, double[] weights) {
        this.id = id;
        this.weights = weights;
        for (double weight : weights) {
            this.count++;
        }
    }

    @Override
    public int compareTo(WeightedInput o) {
        return Long.compare(id, o.id);
    }

    SimilarityResult sumSquareDeltaSkip(double similarityCutoff, WeightedInput other, double skipValue) {
        int len = Math.min(weights.length, other.weights.length);
        double sumSquareDelta = Intersections.sumSquareDeltaSkip(weights, other.weights, len, skipValue);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, sumSquareDelta);
    }

    SimilarityResult sumSquareDelta(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double sumSquareDelta = Intersections.sumSquareDelta(weights, other.weights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, sumSquareDelta);
    }

    SimilarityResult cosineSquaresSkip(double similarityCutoff, WeightedInput other, double skipValue) {
        int len = Math.min(weights.length, other.weights.length);
        double cosineSquares = Intersections.cosineSquareSkip(weights, other.weights, len, skipValue);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }

    SimilarityResult cosineSquares(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double cosineSquares = Intersections.cosineSquare(weights, other.weights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }

    public SimilarityResult pearsonSquares(double similarityCutoff, WeightedInput other) {
        int len = Math.min(weights.length, other.weights.length);
        double pearsonSquares = Intersections.pearsonSquare(weights, other.weights, len);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (pearsonSquares == 0 || pearsonSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, pearsonSquares);
    }

    public SimilarityResult pearsonSquaresSkip(double similarityCutoff, WeightedInput other, Double skipValue) {
        int len = Math.min(weights.length, other.weights.length);
        double cosineSquares = Intersections.pearsonSquareSkip(weights, other.weights, len, skipValue);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }
}
