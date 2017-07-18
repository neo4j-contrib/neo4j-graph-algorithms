package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author mknblch
 */
public abstract class PExporter<T> {

    public static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors() * 2;

    public static final int MIN_BATCH_SIZE = 10;

    public static final String EXPORT_TASK = "EXPORT";

    private final GraphDatabaseAPI db;

    private final ExecutorService executorService;

    private ThreadToStatementContextBridge ctxBridge;

    private int concurrency = DEFAULT_CONCURRENCY;

    private AtomicInteger progress = new AtomicInteger(0);

    private final ProgressLogger progressLogger;

    protected int nodeCount;

    protected final IdMapping idMapping;

    protected final int writePropertyId;


    public PExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty) {
        this.db = Objects.requireNonNull(db);
        this.idMapping = Objects.requireNonNull(idMapping);
        this.progressLogger = new ProgressLoggerAdapter(Objects.requireNonNull(log), EXPORT_TASK);
        this.executorService = null;
        this.ctxBridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        nodeCount = idMapping.nodeCount();
        writePropertyId = getOrCreatePropertyId(writeProperty);
    }

    public PExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        this.db = Objects.requireNonNull(db);
        this.progressLogger = new ProgressLoggerAdapter(Objects.requireNonNull(log), EXPORT_TASK);
        this.idMapping = Objects.requireNonNull(idMapping);
        this.executorService = Objects.requireNonNull(executorService);
        this.ctxBridge = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        nodeCount = idMapping.nodeCount();
        writePropertyId = getOrCreatePropertyId(writeProperty);
    }

    public void write(T data) {
        progress.set(0);
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(data);
        } else {
            writeSequential(data);
        }
    }

    protected int getOrCreatePropertyId(String propertyName) {
        return tokenWriteInTx(tokenWriteOperations -> {
            try {
                return tokenWriteOperations
                        .propertyKeyGetOrCreateForName(propertyName);
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected int getOrCreateRelationshipId(String relationshipName) {
        return tokenWriteInTx(tokenWriteOperations -> {
            try {
                return tokenWriteOperations
                        .relationshipTypeGetOrCreateForName(relationshipName);
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void readInTransaction(Consumer<ReadOperations> consumer) {
        try (Transaction tx = db.beginTx();
             Statement statement = ctxBridge.get()) {
            consumer.accept(statement.readOperations());
            tx.success();
        }
    }

    protected void writeInTransaction(Consumer<DataWriteOperations> consumer) {
        try (Transaction tx = db.beginTx();
             Statement statement = ctxBridge.get()) {
            consumer.accept(statement.dataWriteOperations());
            tx.success();
        } catch (InvalidTransactionTypeKernelException e) {
            throw new RuntimeException(e);
        }
    }

    protected <V> V tokenWriteInTx(Function<TokenWriteOperations, V> consumer) {
        try (Transaction tx = db.beginTx();
             Statement statement = ctxBridge.get()) {
            V v = consumer.apply(statement.tokenWriteOperations());
            tx.success();
            return v;
        }
    }

    private void writeSequential(T data) {
        verify();
        try (Transaction tx = db.beginTx();
             Statement statement = ctxBridge.get()) {
            DataWriteOperations ops = statement.dataWriteOperations();
            for (int i = 0; i < nodeCount; i++) {
                doWrite(ops, data, i);
                progressLogger.logProgress(i, nodeCount - 1);
            }
            tx.success();
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeParallel(T data) {
        verify();
        final int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, MIN_BATCH_SIZE);
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int start = i;
            final int end = Math.min(i + batchSize, nodeCount);
            // create exporter runnables
            runnables.add(() -> {
                try (Transaction tx = db.beginTx();
                     Statement statement = ctxBridge.get()) {
                    DataWriteOperations ops = statement.dataWriteOperations();
                    for (int j = start; j < end; j++) {
                        doWrite(ops, data, j);
                        progressLogger.logProgress(progress.addAndGet(end - j), nodeCount - 1);
                    }
                    tx.success();
                } catch (KernelException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        ParallelUtil.run(runnables, executorService);
    }

    private void verify() {
        if (-1 == writePropertyId) {
            throw new IllegalStateException("no write property id is set");
        }
    }

    protected abstract void doWrite(DataWriteOperations writeOperations, T data, int offset) throws KernelException;

}
