package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ExecutorService;

public abstract class ParallelExporter<T> extends Exporter<T> {

    private final ExecutorService executor;

    private final BatchNodeIterable batchNodes;
    private final int batchSize;

    public ParallelExporter(
            int batchSize,
            GraphDatabaseAPI api,
            BatchNodeIterable batchNodes,
            ExecutorService executor) {
        super(api);
        this.batchNodes = batchNodes;
        this.executor = executor;
        this.batchSize = batchSize;
    }

    @Override
    public final void write(T data) {
        ParallelGraphExporter exporter = newParallelExporter(data);
        ParallelUtil.writeParallel(batchSize, batchNodes, api, exporter, executor);
    }

    protected abstract ParallelGraphExporter newParallelExporter(T data);
}
