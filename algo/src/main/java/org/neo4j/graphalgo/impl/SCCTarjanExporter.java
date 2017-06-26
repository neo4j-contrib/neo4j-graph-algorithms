package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class SCCTarjanExporter extends Exporter<ObjectArrayList<IntSet>> {

    private IdMapping idMapping;
    private int writePropertyId;
    private AtomicInteger set = new AtomicInteger(0);

    public SCCTarjanExporter(GraphDatabaseAPI api) {
        super(api);
    }

    public SCCTarjanExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    public SCCTarjanExporter withWriteProperty(String writeProperty) {
        writeInTransaction(write -> {
            try {
                this.writePropertyId = write.propertyKeyGetOrCreateForName(writeProperty);
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });
        return this;
    }

    @Override
    public void write(ObjectArrayList<IntSet> data) {
        writeInTransaction(write -> {
            for (ObjectCursor<IntSet> oCursor : data) {
                final DefinedProperty property =
                        DefinedProperty.intProperty(writePropertyId, set.incrementAndGet());
                oCursor.value.forEach((Consumer<IntCursor>) iCursor -> {
                    try {
                        write.nodeSetProperty(idMapping.toOriginalNodeId(iCursor.value),
                                property);
                    } catch (EntityNotFoundException
                            | ConstraintValidationKernelException
                            | InvalidTransactionTypeKernelException
                            | AutoIndexingKernelException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        });
    }
}
