package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ParallelExporter;
import org.neo4j.graphalgo.core.utils.ParallelGraphExporter;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public final class BetweennessCentralityExporter extends ParallelExporter<double[]> {

    private final IdMapping idMapping;
    private final int propertyId;

    public BetweennessCentralityExporter(
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
    protected ParallelGraphExporter newParallelExporter(double[] data) {
        return (ParallelGraphExporter.Simple) ((ops, nodeId) -> {
            ops.nodeSetProperty(
                    idMapping.toOriginalNodeId(nodeId),
                    DefinedProperty.doubleProperty(propertyId, data[nodeId])
            );
        });
    }

    public static class NodeBatch implements BatchNodeIterable {

        public final int nodeCount;

        public NodeBatch(int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
            ArrayList<PrimitiveIntIterable> result = new ArrayList<>();
            for (int i = 0; i < nodeCount; i += batchSize) {
                int end = i + batchSize > nodeCount ? nodeCount : i + batchSize;
                result.add(new BatchedNodeIterator(i, end));
            }
            return result;
        }

    }

    public static class BatchedNodeIterator implements PrimitiveIntIterator, PrimitiveIntIterable {

        private final int end;
        private int current;

        private BatchedNodeIterator(int start, int end) {
            this.end = end;
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return current < end;
        }

        @Override
        public int next() {
            return current++;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return this;
        }
    }
}
