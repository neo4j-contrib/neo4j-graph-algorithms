package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;

import java.util.stream.DoubleStream;

class WeightedInput implements  Comparable<WeightedInput> {
    long id;
    double[] weights;
    int count;

    public WeightedInput(long id, double[] weights) {
        this.id = id;
        this.weights = weights;
        for (double weight : weights) {
            if (weight!=0d) this.count++;
        }
    }

    @Override
    public int compareTo(WeightedInput o) {
        return Long.compare(id, o.id);
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
}
