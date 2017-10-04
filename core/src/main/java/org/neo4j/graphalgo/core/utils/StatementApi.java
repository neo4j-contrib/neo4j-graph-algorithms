package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class StatementApi {

    public interface Consumer<E extends Exception> {
        void accept(Statement statement) throws E;
    }

    public interface Function<T, E extends Exception> {
        T apply(Statement statement) throws E;
    }

    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;

    protected StatementApi(GraphDatabaseAPI api) {
        this.api = api;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
    }

    public final <T, E extends Exception> T applyInTransaction(Function<T, E> fun)
    throws E {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            final T result = fun.apply(statement);
            tx.success();
            return result;
        }
    }

    public final <E extends Exception> void acceptInTransaction(Consumer<E> fun)
    throws E {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            fun.accept(statement);
            tx.success();
        }
    }
}
