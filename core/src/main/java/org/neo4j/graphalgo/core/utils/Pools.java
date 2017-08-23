package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Pools {
    public static final int DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_QUEUE_SIZE = DEFAULT_CONCURRENCY * 50;
    public final static ExecutorService DEFAULT = createDefaultPool();

    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool() {
        int threads = DEFAULT_CONCURRENCY * 2;
        return new ThreadPoolExecutor(
                threads / 2,
                threads,
                30L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE),
                new CallerBlocksPolicy());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                // block caller for 100ns
                LockSupport.parkNanos(100);
                try {
                    // submit again
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int getNoThreadsInDefaultPool() {
        return DEFAULT_CONCURRENCY;
    }
}
