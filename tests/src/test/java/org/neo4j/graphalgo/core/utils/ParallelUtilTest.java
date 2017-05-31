package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.graphalgo.api.BatchNodeIterable;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
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

    private PrimitiveIntIterable ints(int from, int size) {
        final PrimitiveIntStack stack = new PrimitiveIntStack(size);
        for (int i = 0; i < size; i++) {
            stack.push(i + from);
        }
        return stack;
    }
}
