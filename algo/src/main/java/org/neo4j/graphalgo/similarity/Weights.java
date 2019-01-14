package org.neo4j.graphalgo.similarity;

import java.util.Arrays;
import java.util.List;

public class Weights {
    public static final long REPEAT_CUTOFF = 3L;

    public static double[] buildWeights(List<Number> weightList) {
        double[] weights = new double[weightList.size()];
        int i = 0;
        for (Number value : weightList) {
            weights[i++] = value.doubleValue();
        }
        return weights;
    }

    public static double[] buildRleWeights(List<Number> weightList, int limit) {
        double[] weights = new double[weightList.size() + (weightList.size() / (limit * 2))];

        double latestValue = Double.POSITIVE_INFINITY;
        int counter = 0;

        int i = 0;
        for (Number value : weightList) {
            if (value.doubleValue() == latestValue || (Double.isNaN(latestValue) && Double.isNaN(value.doubleValue()))) {
                counter++;
            } else {
                if (counter > limit) {
                    weights[i++] = Double.POSITIVE_INFINITY;
                    weights[i++] = counter;
                    weights[i++] = latestValue;
                    counter = 1;
                } else {
                    if (counter > 0) {
                        for (int j = 0; j < counter; j++) {
                            weights[i++] = latestValue;
                        }
                    }
                    counter = 1;
                }
                latestValue = value.doubleValue();
            }
        }

        if (counter > limit) {
            weights[i++] = Double.POSITIVE_INFINITY;
            weights[i++] = counter;
            weights[i++] = latestValue;
        } else {
            for (int j = 0; j < counter; j++) {
                weights[i++] = latestValue;
            }
        }

        return Arrays.copyOf(weights, i);
    }

    public static double[] buildRleWeights(double[] weightList, int limit) {
        double[] weights = new double[weightList.length + (weightList.length / (limit * 2))];

        double latestValue = Double.POSITIVE_INFINITY;
        int counter = 0;

        int i = 0;
        for (double value : weightList) {
            if (value == latestValue || (Double.isNaN(latestValue) && Double.isNaN(value))) {
                counter++;
            } else {
                if (counter > limit) {
                    weights[i++] = Double.POSITIVE_INFINITY;
                    weights[i++] = counter;
                    weights[i++] = latestValue;
                    counter = 1;
                } else {
                    if (counter > 0) {
                        for (int j = 0; j < counter; j++) {
                            weights[i++] = latestValue;
                        }
                    }
                    counter = 1;
                }
                latestValue = value;
            }
        }

        if (counter > limit) {
            weights[i++] = Double.POSITIVE_INFINITY;
            weights[i++] = counter;
            weights[i++] = latestValue;
        } else {
            for (int j = 0; j < counter; j++) {
                weights[i++] = latestValue;
            }
        }

        return Arrays.copyOf(weights, i);
    }
}
