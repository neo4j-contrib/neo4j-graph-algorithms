package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

}
