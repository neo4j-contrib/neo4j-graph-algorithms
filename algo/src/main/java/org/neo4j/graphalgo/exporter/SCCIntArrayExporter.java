package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.IntValueParallelExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class SCCIntArrayExporter extends IntValueParallelExporter<int[]> {

    public SCCIntArrayExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected int toInt(final int[] data, final int offset) {
        return data[offset];
    }

    @Override
    protected boolean hasData(final int[] data, final int offset) {
        return data[offset] != -1;
    }
}
