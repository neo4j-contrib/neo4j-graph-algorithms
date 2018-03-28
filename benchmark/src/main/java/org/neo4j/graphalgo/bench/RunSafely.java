package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.utils.Pools;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface RunSafely extends Runnable {
    default void runSafely() throws Throwable {
        try {
            run();
        } catch (StackOverflowError e) {
            Throwable error = e;
            Pools.DEFAULT.shutdownNow();
            try {
                Pools.DEFAULT.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException e1) {
                e1.addSuppressed(e);
                error = e1;
            }
            throw error;
        }
    }

    static void runSafe(RunSafely run) throws Throwable {
        run.runSafely();
    }
}
