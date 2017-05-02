package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.cursors.IntDoubleCursor;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class LabelPropagationExporter extends Exporter<IntDoubleMap> {

    private final IdMapping idMapping;
    private final int propertyId;

    public LabelPropagationExporter(
            GraphDatabaseAPI api,
            IdMapping idMapping,
            String targetProperty) {
        super(api);
        this.idMapping = idMapping;
        propertyId = getOrCreatePropertyId(targetProperty);
    }

    @Override
    public void write(final IntDoubleMap labels) {
        writeInTransaction(ops -> {
            try {
                for (IntDoubleCursor label : labels) {
                    long nodeId = idMapping.toOriginalNodeId(label.key);
                    ops.nodeSetProperty(
                            nodeId,
                            DefinedProperty.doubleProperty(propertyId, label.value)
                    );
                }
            } catch (EntityNotFoundException | AutoIndexingKernelException | ConstraintValidationKernelException | InvalidTransactionTypeKernelException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
