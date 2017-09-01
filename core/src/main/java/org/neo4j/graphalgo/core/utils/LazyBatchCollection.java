package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.AbstractIterator;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

public final class LazyBatchCollection<T> extends AbstractCollection<T> {

    public interface BatchSupplier<T> {
        T newBatch(long start, long length);
    }

    public static <T> Collection<T> of(
            long nodeCount,
            int batchSize,
            BatchSupplier<T> supplier) {
        return new LazyBatchCollection<>(batchSize, nodeCount, supplier);
    }

    private final BatchSupplier<T> supplier;
    private final long nodeCount;
    private final int batchSize;
    private final int numberOfBatches;

    private LazyBatchCollection(
            int batchSize,
            long nodeCount,
            BatchSupplier<T> supplier) {
        this.supplier = supplier;
        this.nodeCount = nodeCount;
        this.batchSize = batchSize;
        numberOfBatches = ParallelUtil.threadSize(batchSize, nodeCount);
    }

    @Override
    public final Iterator<T> iterator() {
        return new AbstractIterator<T>() {
            private int i;
            private long start;

            @Override
            protected T fetch() {
                if (i++ >= numberOfBatches) {
                    return done();
                }
                long start = this.start;
                this.start += batchSize;
                long length = Math.min(batchSize, nodeCount - start);
                return supplier.newBatch(start, length);
            }
        };
    }

    @Override
    public final int size() {
        return numberOfBatches;
    }
}
