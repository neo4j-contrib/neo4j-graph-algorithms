package org.neo4j.graphalgo.impl.results;

import java.util.Arrays;

public class NormalizationComputations {
    public static double max(double[][] partitions) {
        double max = 1.0;
        for (double[] partition : partitions) {
            max = Math.max(max, NormalizationComputations.max(partition, max));
        }
        return max;
    }

    public static double l2Norm(double[][] partitions) {
        double sum = 0.0;
        for (double[] partition : partitions) {
            sum += squaredSum(partition);
        }
        return Math.sqrt(sum);
    }

    static double l1Norm(double[][] partitions) {
        double sum = 0.0;
        for (double[] partition : partitions) {
            sum += l1Norm(partition);
        }
        return sum;
    }

    static double squaredSum(double[] partition) {
        return Arrays.stream(partition).parallel().map(value -> value * value).sum();
    }

    static double l1Norm(double[] partition) {
        return Arrays.stream(partition).parallel().sum();
    }

    public static double max(double[] result, double defaultMax) {
        return Arrays.stream(result).parallel().max().orElse(defaultMax);
    }
}
