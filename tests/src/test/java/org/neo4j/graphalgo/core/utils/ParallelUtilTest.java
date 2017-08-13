package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.graphalgo.api.BatchNodeIterable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class ParallelUtilTest extends RandomizedTest {

    @Test
    public void threadSizeShouldDivideByBatchsize() throws Exception {
        int batchSize = between(10, 10_000);
        int threads = between(10, 10_000);
        assertEquals(
                threads,
                ParallelUtil.threadSize(batchSize, batchSize * threads));
    }

    @Test
    public void threadSizeShouldReturnEnoughThreadsForIncompleteBatches() throws
            Exception {
        int batchSize = between(10, 10_000);
        int threads = between(10, 10_000);
        int elements = batchSize * threads + between(1, batchSize - 1);
        assertEquals(threads + 1, ParallelUtil.threadSize(batchSize, elements));
    }

    @Test
    public void threadSizeShouldReturn1ForNonCompleteBatches() throws
            Exception {
        int batchSize = between(2, 1_000_000);
        assertEquals(
                1,
                ParallelUtil.threadSize(
                        batchSize,
                        batchSize - randomInt(batchSize)));
    }

    @Test
    public void threadSizeShouldReturn1ForZeroElements() throws Exception {
        int batchSize = between(2, 1_000_000);
        assertEquals(1, ParallelUtil.threadSize(batchSize, 0));
    }

    @Test
    public void threadSizeShouldFailForZeroBatchsize() throws Exception {
        try {
            ParallelUtil.threadSize(0, randomInt());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: 0", e.getMessage());
        }
    }

    @Test
    public void threadSizeShouldFailForNegativeBatchsize() throws Exception {
        int batchSize = between(Integer.MIN_VALUE, -1);
        try {
            ParallelUtil.threadSize(batchSize, randomInt());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: " + batchSize, e.getMessage());
        }
    }

    @Test
    public void shouldRunBatchesSequentialIfNoExecutorIsGiven() {
        PrimitiveIntIterable[] ints = {ints(0, 10), ints(10, 14)};
        BatchNodeIterable batches = (size) -> Arrays.asList(ints);
        Runnable task = () -> {
        };
        ParallelGraphImporter importer = mock(ParallelGraphImporter.class);
        when(importer.newImporter(anyInt(), any())).thenReturn(task);

        final Collection tasks = ParallelUtil.readParallel(
                10,
                batches,
                importer,
                null);

        verify(importer, times(1)).newImporter(0, ints[0]);
        verify(importer, times(1)).newImporter(10, ints[1]);
        assertEquals(2, tasks.size());
        for (Object t : tasks) {
            assertSame(task, t);
        }
    }

    @Test
    public void batchingShouldCatenatePartitions() throws Exception {
        int minBatchSize = randomIntBetween(5, 25);
        int maxConcurrency = randomIntBetween(2, 10);
        int nodeCount = randomIntBetween(500, 1500);

        String params = String.format(
                " [bs=%d,c=%d,n=%d]",
                minBatchSize,
                maxConcurrency,
                nodeCount);

        int batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                maxConcurrency,
                minBatchSize);

        assertTrue(
                "batchSize smaller than minSize" + params,
                batchSize >= minBatchSize);
        assertTrue(
                "batchSize too small to satisfy desired concurrency" + params,
                (int) Math.ceil(nodeCount / (double) batchSize) <= maxConcurrency);
    }

    @Test
    public void shouldRunAtMostConcurrencyTasks() throws Exception {
        int tasks = 4;
        int concurrency = 2;

        AtomicInteger running = new AtomicInteger();
        AtomicInteger started = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(tasks);

        List<CountDownLatch> waitingForStart = latches(tasks + (tasks - concurrency));
        List<CountDownLatch> simulateRunning = latches(tasks);

        List<Runnable> runnables = IntStream.range(0, tasks)
                .mapToObj(index -> (Runnable) () -> {
                    running.incrementAndGet();
                    started.incrementAndGet();
                    waitingForStart.get(index).countDown();
                    try {
                        simulateRunning.get(index).await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    running.decrementAndGet();
                    if (index >= concurrency) {
                        waitingForStart.get(index + concurrency).countDown();
                    }
                })
                .collect(Collectors.toList());

        Thread thread = new Thread(() -> ParallelUtil
                .runWithConcurrency(
                        concurrency,
                        runnables,
                        pool
                )
        );
        thread.start();

        try {

            int i;
            for (i = 0; i < concurrency; i++) {
                waitingForStart.get(i).await();
            }

            for (int j = 0; j < (tasks - concurrency); j++) {
                assertEquals(
                        concurrency + " tasks currently running",
                        concurrency,
                        running.get());
                assertEquals(i + " tasks have been started", i, started.get());

                simulateRunning.get(j).countDown();
                waitingForStart.get(i++).await();
            }

            for (int j = tasks - concurrency; j < tasks; j++) {
                assertEquals(
                        (tasks - j) + " tasks currently running",
                        tasks - j,
                        running.get());
                assertEquals(
                        tasks + " tasks have been started",
                        tasks,
                        started.get());

                simulateRunning.get(j).countDown();
                waitingForStart.get(i++).await();
            }

            assertEquals("0 tasks currently running", 0, running.get());
            assertEquals(
                    tasks + " tasks have been started",
                    tasks,
                    started.get());

            thread.join();
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void shouldRunAtMostConcurrencyWithTerminationTasks() throws
            Exception {
        int tasks = 4;
        int concurrency = 2;

        AtomicBoolean isRunning = new AtomicBoolean(true);
        TerminationFlag runningFlag = isRunning::get;

        AtomicInteger running = new AtomicInteger();
        AtomicInteger started = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(tasks);

        List<CountDownLatch> waitingForStart = latches(tasks + (tasks - concurrency));
        List<CountDownLatch> simulateRunning = latches(tasks);

        List<Runnable> runnables = IntStream.range(0, tasks)
                .mapToObj(index -> (Runnable) () -> {
                    running.incrementAndGet();
                    started.incrementAndGet();
                    waitingForStart.get(index).countDown();
                    try {
                        simulateRunning.get(index).await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    running.decrementAndGet();
                    if (index >= concurrency) {
                        waitingForStart.get(index + concurrency).countDown();
                    }
                })
                .collect(Collectors.toList());

        Thread thread = new Thread(() -> ParallelUtil
                .runWithConcurrency(
                        concurrency,
                        runnables,
                        runningFlag,
                        pool
                )
        );
        thread.start();

        try {
            // first two already started
            waitingForStart.get(0).await();
            waitingForStart.get(1).await();

            // finish one and let another one start
            simulateRunning.get(0).countDown();
            waitingForStart.get(2).await();

            // abort execution
            isRunning.set(false);
            // let remaining two started finish
            simulateRunning.get(1).countDown();
            simulateRunning.get(2).countDown();

            thread.join();

            assertEquals("0 tasks currently running", 0, running.get());
            assertEquals("3 tasks have been started", 3, started.get());
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    private PrimitiveIntIterable ints(int from, int size) {
        final PrimitiveIntStack stack = new PrimitiveIntStack(size);
        for (int i = 0; i < size; i++) {
            stack.push(i + from);
        }
        return stack;
    }

    private static List<CountDownLatch> latches(int size) {
        return IntStream.range(0, size)
                .mapToObj(i -> new CountDownLatch(1))
                .collect(Collectors.toList());
    }
}
