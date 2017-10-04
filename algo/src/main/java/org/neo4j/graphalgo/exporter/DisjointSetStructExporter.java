package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.IntValueParallelExporter;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class DisjointSetStructExporter extends IntValueParallelExporter<DisjointSetStruct> {

    public DisjointSetStructExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected int toInt(final DisjointSetStruct data, final int offset) {
        return data.findNoOpt(offset);
    }
}
