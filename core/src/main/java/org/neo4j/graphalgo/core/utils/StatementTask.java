package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.Callable;

public abstract class StatementTask<T, E extends Exception> implements RenamingRunnable, Callable<T> {

    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;

    protected StatementTask(GraphDatabaseAPI api) {
        this.api = api;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    @Override
    public T call() throws E {
        return runAndReturn();
    }

    @Override
    public void doRun() {
        try {
            runAndReturn();
        } catch (Exception e) {
            throw Exceptions.launderedException(e);
        }
    }


    private T runAndReturn() throws E {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            final T result = runWithStatement(statement);
            tx.success();
            return result;
        }
    }

    protected abstract T runWithStatement(Statement statement) throws E;
}
