package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.graphalgo.core.utils.ParallelGraphExporter;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class ParallelBetweennessCentralityExporter extends ParallelExporter<AtomicDoubleArray> {

    private final IdMapping idMapping;
    private final int propertyId;

    public ParallelBetweennessCentralityExporter(
            int batchSize,
            GraphDatabaseAPI api,
            IdMapping idMapping,
            BatchNodeIterable batchNodes,
            String targetProperty,
            ExecutorService executor) {
        super(batchSize, api, batchNodes, executor);
        this.idMapping = idMapping;
        propertyId = getOrCreatePropertyId(targetProperty);
    }

    @Override
    protected ParallelGraphExporter newParallelExporter(AtomicDoubleArray data) {
        return (ParallelGraphExporter.Simple) ((ops, nodeId) -> {
            ops.nodeSetProperty(
                    idMapping.toOriginalNodeId(nodeId),
                    DefinedProperty.doubleProperty(propertyId, data.get(nodeId))
            );
        });
    }
}
