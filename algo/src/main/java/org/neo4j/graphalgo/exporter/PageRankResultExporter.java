package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.DoubleValueParallelExporter;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class PageRankResultExporter extends DoubleValueParallelExporter<PageRankResult> {

    public PageRankResultExporter(
            GraphDatabaseAPI db,
            IdMapping idMapping,
            Log log,
            String writeProperty,
            ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected double toDouble(final PageRankResult data, final int offset) {
        return data.score(offset);
    }
}


