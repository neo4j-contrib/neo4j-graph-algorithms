package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.core.utils.HugeParallelExporter;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class HugePageRankResultExporter extends HugeParallelExporter<PageRankResult> {

    public HugePageRankResultExporter(
            GraphDatabaseAPI db,
            HugeIdMapping idMapping,
            Log log,
            String writeProperty) {
        super(db, idMapping, log, writeProperty);
    }

    public HugePageRankResultExporter(
            GraphDatabaseAPI db,
            HugeIdMapping idMapping,
            Log log,
            String writeProperty,
            ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected void doWrite(
            DataWriteOperations writeOperations,
            PageRankResult data,
            long offset) throws KernelException {
        writeOperations.nodeSetProperty(
                idMapping.toOriginalNodeId(offset),
                DefinedProperty.doubleProperty(
                        writePropertyId,
                        data.score(offset)));
    }
}


