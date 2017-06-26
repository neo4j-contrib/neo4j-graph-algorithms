package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class MultiStepSCCExporter extends Exporter<IntObjectMap<IntSet>> {

    private IdMapping idMapping;
    private int writePropertyId;

    public MultiStepSCCExporter(GraphDatabaseAPI api) {
        super(api);
    }

    public MultiStepSCCExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    public MultiStepSCCExporter withWriteProperty(String writeProperty) {
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
    public void write(IntObjectMap<IntSet> data) {
        writeInTransaction(write -> {
            for (IntObjectCursor<IntSet> oCursor : data) {
                final DefinedProperty property =
                        DefinedProperty.intProperty(writePropertyId, oCursor.key);
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
