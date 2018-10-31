package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

import static org.neo4j.graphalgo.similarity.RleTransformer.REPEAT_CUTOFF;

class RleWeightedInput implements Comparable<RleWeightedInput> {
    private final int initialSize;
    long id;
    double[] weights;
    int count;

    public RleWeightedInput(long id, double[] weights, int initialSize, double skipValue) {
        this.initialSize = initialSize;
        boolean skipNan = Double.isNaN(skipValue);
        this.id = id;
        this.weights = weights;
        for (double weight : RleTransformer.decode(weights, REPEAT_CUTOFF)) {
            if (!(weight == skipValue || (skipNan && Double.isNaN(weight)))) this.count++;
        }
    }

    public RleWeightedInput(long id, double[] weights, int initialSize) {
        this.id = id;
        this.weights = weights;
        this.initialSize = initialSize;
        this.count = initialSize;
//        for (double weight : weights) {
//            this.count++;
//        }
    }

    @Override
    public int compareTo(RleWeightedInput o) {
        return Long.compare(id, o.id);
    }

    SimilarityResult sumSquareDeltaSkip(double similarityCutoff, RleWeightedInput other, double skipValue) {
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

    SimilarityResult sumSquareDelta(double similarityCutoff, RleWeightedInput other) {
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

    SimilarityResult cosineSquaresSkip(double similarityCutoff, RleWeightedInput other, double skipValue) {
        double[] thisWeights = RleTransformer.decode(weights, initialSize);
        double[] otherWeights = RleTransformer.decode(other.weights, initialSize);

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquareSkip(thisWeights, otherWeights, len, skipValue);
        long intersection = 0;
        /* todo
        for (int i = 0; i < len; i++) {
            if (weights[i] == other.weights[i] && weights[i] != 0d) intersection++;
        }
        */
        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, count, other.count, intersection, cosineSquares);
    }

    SimilarityResult cosineSquares(double similarityCutoff, RleWeightedInput other) {
        double[] thisWeights = RleTransformer.decode(weights, initialSize);
        double[] otherWeights = RleTransformer.decode(other.weights, initialSize);

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquare(thisWeights, otherWeights, len);
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
