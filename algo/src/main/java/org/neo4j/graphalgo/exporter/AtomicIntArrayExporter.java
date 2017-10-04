package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.IntValueParallelExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mknblch
 */
public class AtomicIntArrayExporter extends IntValueParallelExporter<AtomicIntegerArray> {

    public AtomicIntArrayExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected int toInt(final AtomicIntegerArray data, final int offset) {
        return data.get(offset);
    }
}


