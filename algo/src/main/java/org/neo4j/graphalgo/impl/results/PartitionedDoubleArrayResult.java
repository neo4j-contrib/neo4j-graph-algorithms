package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import static org.neo4j.graphalgo.core.utils.ArrayUtil.binaryLookup;

public final class PartitionedDoubleArrayResult implements CentralityResult, PropertyTranslator.OfDouble<double[][]> {
    private final double[][] partitions;
    private final long[] starts;

    public PartitionedDoubleArrayResult(
            double[][] partitions,
            long[] starts) {
        this.partitions = partitions;
        this.starts = starts;
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        exporter.write(propertyName, partitions, this);
    }

    @Override
    public double toDouble(final double[][] data, final long nodeId) {
        int idx = binaryLookup(nodeId, starts);
        return data[idx][(int) (nodeId - starts[idx])];
    }

    @Override
    public double score(final long nodeId) {
        return toDouble(partitions, nodeId);
    }

    @Override
    public double score(final int nodeId) {
        return score((long) nodeId);
    }
}
