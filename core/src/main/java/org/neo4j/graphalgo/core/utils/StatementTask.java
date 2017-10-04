package org.neo4j.graphalgo.core.utils;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.Callable;

public abstract class StatementTask<T, E extends Exception> extends StatementApi implements RenamingRunnable, Callable<T>, StatementApi.Function<T, E> {

    protected StatementTask(GraphDatabaseAPI api) {
        super(api);
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
        return applyInTransaction(this);
    }
}
