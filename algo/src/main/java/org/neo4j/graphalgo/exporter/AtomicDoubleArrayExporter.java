package org.neo4j.graphalgo.exporter;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public class AtomicDoubleArrayExporter extends ParallelExporter<AtomicDoubleArray> {

    public AtomicDoubleArrayExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty) {
        super(db, idMapping, log, writeProperty);
    }

    public AtomicDoubleArrayExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected void doWrite(DataWriteOperations writeOperations, AtomicDoubleArray data, int offset) throws KernelException {
        writeOperations.nodeSetProperty(idMapping.toOriginalNodeId(offset),
                DefinedProperty.doubleProperty(writePropertyId, data.get(offset)));
    }
}


