package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

/**
 * @author mknblch
 */
public abstract class PExporter<T> {

    public static final int DEFAULT_CONCURRENCY = 4;

    public static final int MIN_BATCH_SIZE = 10;

    protected GraphDatabaseAPI db;

    protected ExecutorService executorService;

    protected IdMapping idMapping;

    protected int nodeCount;

    protected ThreadToStatementContextBridge ctxBridge;

    protected int concurrency = DEFAULT_CONCURRENCY;

    public PExporter(GraphDatabaseAPI db, ExecutorService executorService, IdMapping idMapping) {
        this.db = db;
        this.executorService = executorService;
        this.idMapping = idMapping;
        nodeCount = idMapping.nodeCount();
    }

    public PExporter withDb(GraphDatabaseAPI db) {
        this.db = db;
        this.ctxBridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        return this;
    }

    public PExporter withExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public PExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    public PExporter withWriteProperty(String writePropertyName) {

        return this;
    }



    public void write(T data) {
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(data);
        } else {
            writeSequential(data);
        }
    }

    private void writeSequential(T data) {
        try (Transaction tx = db.beginTx();
             Statement statement = ctxBridge.get()) {
            DataWriteOperations ops = statement.dataWriteOperations();
            for (int i = 0; i < nodeCount; i++) {
                doWrite(ops, data, i);
            }
            tx.success();
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeParallel(T data) {
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, MIN_BATCH_SIZE);
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int start = i;
            final int end = Math.min(i + batchSize, nodeCount);
            runnables.add(() -> {
                try (Transaction tx = db.beginTx();
                     Statement statement = ctxBridge.get()) {
                    DataWriteOperations ops = statement.dataWriteOperations();
                    for (int j = start; j < end; j++) {
                        doWrite(ops, data, j);
                    }
                    tx.success();
                } catch (KernelException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        ParallelUtil.run(runnables, executorService);
    }

    protected abstract void doWrite(DataWriteOperations writeOperations, T data, int offset);

    interface BatchExportRunnable<T> {

        void write(DataWriteOperations writeOperations, T data, int offset);
    }
}
