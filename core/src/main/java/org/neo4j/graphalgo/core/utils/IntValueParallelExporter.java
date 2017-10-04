package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

public abstract class IntValueParallelExporter<T> extends ParallelExporter<T> {

    public IntValueParallelExporter(
            final GraphDatabaseAPI db,
            final IdMapping idMapping,
            final Log log,
            final String writeProperty,
            final ExecutorService executorService) {
        super(db, idMapping, log, writeProperty, executorService);
    }

    @Override
    protected void doWrite(
            final DataWriteOperations writeOperations,
            final T data,
            final int offset) throws KernelException {
        if (hasData(data, offset)) {
            writeOperations.nodeSetProperty(
                    idMapping.toOriginalNodeId(offset),
                    DefinedProperty.intProperty(
                            writePropertyId,
                            toInt(data, offset)
                    )
            );
        }
    }

    protected boolean hasData(final T data, final int offset) {
        return true;
    }

    protected abstract int toInt(final T data, final int offset);
}
