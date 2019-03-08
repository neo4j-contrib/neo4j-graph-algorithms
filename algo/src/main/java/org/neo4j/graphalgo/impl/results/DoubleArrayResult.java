package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;

public final class DoubleArrayResult implements CentralityResult {
    private final double[] result;

    public DoubleArrayResult(double[] result) {
        this.result = result;
    }

    @Override
    public void export(
            final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                result,
                Translators.DOUBLE_ARRAY_TRANSLATOR);
    }

    @Override
    public double computeMax() {
        return NormalizationComputations.max(result, 1.0);
    }

    @Override
    public double computeL2Norm() {
        return Math.sqrt(NormalizationComputations.squaredSum(result));
    }

    @Override
    public double computeL1Norm() {
        return NormalizationComputations.l1Norm(result);
    }

    @Override
    public final double score(final long nodeId) {
        return result[(int) nodeId];
    }

    @Override
    public double score(final int nodeId) {
        return result[nodeId];
    }
}
