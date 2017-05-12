package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.graphalgo.core.utils.ParallelGraphExporter;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;

public final class LabelPropagationExporter extends ParallelExporter<IntDoubleMap> {

    private final IdMapping idMapping;
    private final int propertyId;

    public LabelPropagationExporter(
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
    protected ParallelGraphExporter newParallelExporter(IntDoubleMap data) {
        return (ParallelGraphExporter.Simple) ((ops, nodeId) -> {
            if (data.containsKey(nodeId)) {
                double score = data.get(nodeId);
                long neoNodeId = idMapping.toOriginalNodeId(nodeId);
                ops.nodeSetProperty(
                    neoNodeId,
                    DefinedProperty.doubleProperty(propertyId, score)
                );
            }
        });
    }
}
