package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;

import java.util.function.Function;

public  class NormalizedCentralityResult implements CentralityResult {
    private CentralityResult result;
    private Function<Double, Double> normalizationFunction;

    public NormalizedCentralityResult(CentralityResult result, Function<Double, Double> normalizationFunction) {
        this.result = result;
        this.normalizationFunction = normalizationFunction;
    }

    @Override
    public void export(String propertyName, Exporter exporter) {
        result.export(propertyName, exporter);
    }

    @Override
    public double score(int nodeId) {
        return normalizationFunction.apply(result.score(nodeId));
    }

    @Override
    public double score(long nodeId) {
        return normalizationFunction.apply(result.score(nodeId));
    }

    @Override
    public double computeMax() {
        return result.computeMax();
    }

    @Override
    public double computeL2Norm() {
        return result.computeL2Norm();
    }

    @Override
    public double computeL1Norm() {
        return result.computeL1Norm();
    }
}
