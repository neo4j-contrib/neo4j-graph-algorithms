package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.HugeBatchNodeIterable;
import org.neo4j.helpers.Exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

public final class ParallelUtil {

    public static final int DEFAULT_BATCH_SIZE = 10_000;

    public static int threadSize(int batchSize, int elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (int) Math.ceil(elementCount / (double) batchSize);
    }

    public static long threadSize(int batchSize, long elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (long) Math.ceil(elementCount / (double) batchSize);
    }

    public static long threadSize(long batchSize, long elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (long) Math.ceil(elementCount / (double) batchSize);
    }

    public static int adjustBatchSize(
            int nodeCount,
            int concurrency,
            int minBatchSize) {
        if (concurrency <= 0) {
            concurrency = nodeCount;
        }
        int targetBatchSize = threadSize(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    public static long adjustBatchSize(
            long nodeCount,
            int concurrency,
            long minBatchSize) {
        if (concurrency <= 0) {
            concurrency = (int) Math.min(nodeCount, (long) Integer.MAX_VALUE);
        }
        long targetBatchSize = threadSize(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    public static boolean canRunInParallel(ExecutorService executor) {
        return executor != null && !(executor.isShutdown() || executor.isTerminated());
    }

    /**
     * Executes read operations in parallel, based on the given batch size
     * and executor.
     */
    public static <T extends Runnable> Collection<T> readParallel(
            int concurrency,
            int batchSize,
            BatchNodeIterable idMapping,
            ParallelGraphImporter<T> importer,
            ExecutorService executor) {

        Collection<PrimitiveIntIterable> iterators =
                idMapping.batchIterables(batchSize);

        int threads = iterators.size();

        if (!canRunInParallel(executor) || threads == 1) {
            int nodeOffset = 0;
            Collection<T> tasks = new ArrayList<>(threads);
            for (PrimitiveIntIterable iterator : iterators) {
                final T task = importer.newImporter(nodeOffset, iterator);
                tasks.add(task);
                task.run();
                nodeOffset += batchSize;
            }
            return tasks;
        } else {
            List<T> tasks = new ArrayList<>(threads);
            int nodeOffset = 0;
            for (PrimitiveIntIterable iterator : iterators) {
                tasks.add(importer.newImporter(nodeOffset, iterator));
                nodeOffset += batchSize;
            }
            runWithConcurrency(concurrency, tasks, executor);
            return tasks;
        }
    }

    /**
     * Executes read operations in parallel, based on the given batch size
     * and executor.
     */
    public static <T extends Runnable> void readParallel(
            int concurrency,
            int batchSize,
            HugeBatchNodeIterable idMapping,
            HugeParallelGraphImporter<T> importer,
            ExecutorService executor) {

        Collection<PrimitiveLongIterable> iterators =
                idMapping.hugeBatchIterables(batchSize);

        int threads = iterators.size();

        if (!canRunInParallel(executor) || threads == 1) {
            long nodeOffset = 0;
            for (PrimitiveLongIterable iterator : iterators) {
                final T task = importer.newImporter(nodeOffset, iterator);
                task.run();
                nodeOffset += batchSize;
            }
        } else {
            AtomicLong nodeOffset = new AtomicLong();
            Collection<T> tasks = LazyMappingCollection.of(
                    iterators,
                    it -> importer.newImporter(nodeOffset.getAndAdd(batchSize), it));
            runWithConcurrency(concurrency, tasks, executor);
        }
    }

    /**
     * Runs a collection of {@link Runnable}s in parallel for their side-effects.
     * The level of parallelism is defined by the given executor.
     * <p>
     * This is similar to {@link ExecutorService#invokeAll(Collection)},
     * except that all Exceptions thrown by any task are chained together.
     */
    public static void run(
            Collection<? extends Runnable> tasks,
            ExecutorService executor) {
        run(tasks, executor, null);
    }

    public static void run(
            Collection<? extends Runnable> tasks,
            ExecutorService executor,
            Collection<Future<?>> futures) {

        if (tasks.size() == 1) {
            tasks.iterator().next().run();
            return;
        }

        if (null == executor) {
            tasks.forEach(Runnable::run);
            return;
        }

        if (executor.isShutdown() || executor.isTerminated()) {
            throw new IllegalStateException("Executor is shut down");
        }

        if (futures == null) {
            futures = new ArrayList<>(tasks.size());
        } else {
            futures.clear();
        }

        for (Runnable task : tasks) {
            futures.add(executor.submit(task));
        }

        awaitTermination(futures);
    }

    public static void run(
            Collection<? extends Runnable> tasks,
            Runnable selfTask,
            ExecutorService executor,
            Collection<Future<?>> futures) {

        if (tasks.size() == 0) {
            selfTask.run();
            return;
        }

        if (null == executor) {
            tasks.forEach(Runnable::run);
            selfTask.run();
            return;
        }

        if (executor.isShutdown() || executor.isTerminated()) {
            throw new IllegalStateException("Executor is shut down");
        }

        if (futures == null) {
            futures = new ArrayList<>(tasks.size());
        } else {
            futures.clear();
        }

        for (Runnable task : tasks) {
            futures.add(executor.submit(task));
        }

        awaitTermination(futures);
    }

    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                TerminationFlag.RUNNING_TRUE,
                executor);
    }

    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            TerminationFlag terminationFlag,
            ExecutorService executor) {

        if (!canRunInParallel(executor)
                || tasks.size() == 1
                || concurrency <= 1) {
            Iterator<? extends Runnable> iterator = tasks.iterator();
            while (iterator.hasNext() && terminationFlag.running()) {
                iterator.next().run();
            }
            return;
        }

        CompletionService completionService =
                new CompletionService(executor);

        Iterator<? extends Runnable> ts = tasks.iterator();

        Throwable error = null;
        // generally assumes that tasks.size is notably larger than concurrency
        try {
            // add first concurrency tasks
            while (concurrency > 0 && ts.hasNext() && terminationFlag.running()) {
                Runnable next = ts.next();
                completionService.submit(next);
                concurrency--;
            }

            // submit all remaining tasks
            while (ts.hasNext() && terminationFlag.running()) {
                try {
                    completionService.awaitNext();
                } catch (ExecutionException e) {
                    error = Exceptions.chain(error, e.getCause());
                } catch (CancellationException ignore) {
                }
                if (terminationFlag.running()) {
                    Runnable next = ts.next();
                    completionService.submit(next);
                }
            }

            // wait for all tasks to finish
            while (completionService.hasTasks() && terminationFlag.running()) {
                try {
                    completionService.awaitNext();
                } catch (ExecutionException e) {
                    error = Exceptions.chain(error, e.getCause());
                } catch (CancellationException ignore) {
                }
            }
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            // cancel all regardless of done flag because we could have aborted
            // from the termination flag
            completionService.cancelAll();
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void awaitTermination(Collection<Future<?>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void awaitTerminations(Queue<Future<?>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            while (!futures.isEmpty()) {
                try {
                    futures.poll().get();
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void iterateParallel(
            ExecutorService executorService,
            int size,
            int concurrency,
            IntConsumer consumer) {
        final List<Future<?>> futures = new ArrayList<>();
        final int batchSize = size / concurrency;
        for (int i = 0; i < size; i += batchSize) {
            final int start = i;
            final int end = Math.min(size, start + batchSize);
            futures.add(executorService.submit(() -> {
                for (int j = start; j < end; j++) {
                    consumer.accept(j);
                }
            }));
        }
        awaitTermination(futures);
    }


    /**
     * Copied from {@link java.util.concurrent.ExecutorCompletionService}
     * and adapted to reduce indirection.
     * Does not support {@link java.util.concurrent.ForkJoinPool} as backing executor.
     */
    private static final class CompletionService {
        private final Executor executor;
        private final Set<Future<Void>> running;
        private final BlockingQueue<Future<Void>> completionQueue;

        private class QueueingFuture extends FutureTask<Void> {
            QueueingFuture(final Runnable runnable) {
                super(runnable, null);
                running.add(this);
            }

            @Override
            protected void done() {
                running.remove(this);
                if (!isCancelled()) {
                    completionQueue.add(this);
                }
            }
        }

        CompletionService(ExecutorService executor) {
            if (!canRunInParallel(executor)) {
                throw new IllegalArgumentException(
                        "executor already terminated or not usable");
            }
            this.executor = executor;
            this.completionQueue = new LinkedBlockingQueue<>();
            this.running = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }

        void submit(Runnable task) {
            Objects.requireNonNull(task);
            QueueingFuture future = new QueueingFuture(task);
            executor.execute(future);
        }

        boolean hasTasks() {
            return !(running.isEmpty() && completionQueue.isEmpty());
        }

        void awaitNext() throws InterruptedException, ExecutionException {
            completionQueue.take().get();
        }

        void cancelAll() {
            stopFutures(running);
            stopFutures(completionQueue);
        }

        private void stopFutures(final Collection<Future<Void>> futures) {
            for (Future<Void> future : futures) {
                future.cancel(true);
            }
            futures.clear();
        }
    }
}
