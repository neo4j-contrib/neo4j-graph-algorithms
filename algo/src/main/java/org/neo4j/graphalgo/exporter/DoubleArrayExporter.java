package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.DoubleValueParallelExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class DoubleArrayExporter extends DoubleValueParallelExporter<double[]> {

    public DoubleArrayExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected double toDouble(final double[] data, final int offset) {
        return data[offset];
    }
}


