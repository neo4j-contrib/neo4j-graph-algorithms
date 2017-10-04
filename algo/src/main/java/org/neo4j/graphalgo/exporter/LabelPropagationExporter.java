package org.neo4j.graphalgo.exporter;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.DoubleValueParallelExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class LabelPropagationExporter extends DoubleValueParallelExporter<IntDoubleMap> {

    public LabelPropagationExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected double toDouble(final IntDoubleMap data, final int offset) {
        return data.get(offset);
    }

    @Override
    protected boolean hasData(final IntDoubleMap data, final int offset) {
        return data.containsKey(offset);
    }
}


