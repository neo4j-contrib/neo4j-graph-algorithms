package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.graphalgo.core.utils.ParallelGraphExporter;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;

public final class PageRankExporter extends ParallelExporter<double[]> {

    private final IdMapping idMapping;
    private final int propertyId;

    public PageRankExporter(
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
    protected ParallelGraphExporter newParallelExporter(final double[] data) {
        return (ParallelGraphExporter.Simple) ((ops, nodeId) -> {
            double score = data[nodeId];
            long neoNodeId = idMapping.toOriginalNodeId(nodeId);
            ops.nodeSetProperty(
                    neoNodeId,
                    DefinedProperty.doubleProperty(propertyId, score)
            );
        });
    }
}
