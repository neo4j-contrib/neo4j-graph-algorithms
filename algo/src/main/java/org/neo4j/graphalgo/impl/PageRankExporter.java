package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.Exporter;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class PageRankExporter extends Exporter<double[]> {

    private final IdMapping idMapping;
    private final int propertyId;

    public PageRankExporter(
            GraphDatabaseAPI api,
            IdMapping idMapping,
            String targetProperty) {
        super(api);
        this.idMapping = idMapping;
        propertyId = getOrCreatePropertyId(targetProperty);
    }

    @Override
    public void write(final double[] scores) {
        writeInTransaction(ops -> {
            try {
                for (int i = 0, l = scores.length; i < l; i++) {
                    double score = scores[i];
                    long nodeId = idMapping.toOriginalNodeId(i);
                    ops.nodeSetProperty(
                            nodeId,
                            DefinedProperty.doubleProperty(propertyId, score)
                    );
                }
            } catch (EntityNotFoundException | AutoIndexingKernelException | ConstraintValidationKernelException | InvalidTransactionTypeKernelException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
