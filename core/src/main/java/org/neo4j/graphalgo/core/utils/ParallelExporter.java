/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mknblch
 */
public abstract class ParallelExporter<T> extends AbstractExporter<T> {

    public static final int MIN_BATCH_SIZE = 10_000;
    public static final int MAX_BATCH_SIZE = 100_000;

    public static final String TASK_EXPORT = "EXPORT";

    private final ExecutorService executorService;

    private int concurrency = Pools.DEFAULT_CONCURRENCY;

    private AtomicInteger progress = new AtomicInteger(0);

    private final ProgressLogger progressLogger;

    protected int nodeCount;

    protected final IdMapping idMapping;

    protected final int writePropertyId;


    public ParallelExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty) {
        super(Objects.requireNonNull(db));
        this.idMapping = Objects.requireNonNull(idMapping);
        this.progressLogger = new ProgressLoggerAdapter(Objects.requireNonNull(log), TASK_EXPORT);
        this.executorService = null;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        writePropertyId = getOrCreatePropertyId(writeProperty);
    }

    public ParallelExporter(GraphDatabaseAPI db, IdMapping idMapping, Log log, String writeProperty, ExecutorService executorService) {
        super(Objects.requireNonNull(db));
        this.progressLogger = new ProgressLoggerAdapter(Objects.requireNonNull(log), TASK_EXPORT);
        this.idMapping = Objects.requireNonNull(idMapping);
        this.executorService = Objects.requireNonNull(executorService);
        nodeCount = Math.toIntExact(idMapping.nodeCount());
        writePropertyId = getOrCreatePropertyId(writeProperty);
    }

    public ParallelExporter<T> withConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    public void write(T data) {
        progress.set(0);
        if (ParallelUtil.canRunInParallel(executorService)) {
            writeParallel(data);
        } else {
            writeSequential(data);
        }
    }

    private void writeSequential(T data) {
        verify();
        try (Transaction tx = api.beginTx();
             Statement statement = bridge.get()) {
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
        final int batchSize = Math.min(MAX_BATCH_SIZE, ParallelUtil.adjustBatchSize(nodeCount, concurrency, MIN_BATCH_SIZE));
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            final int start = i;
            final int end = Math.min(i + batchSize, nodeCount);
            // create exporter runnables
            runnables.add(() -> {
                try (Transaction tx = api.beginTx();
                     Statement statement = bridge.get()) {
                    DataWriteOperations ops = statement.dataWriteOperations();
                    for (int j = start; j < end; j++) {
                        doWrite(ops, data, j);
                        progressLogger.logProgress(progress.getAndIncrement(), nodeCount - 1);
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
