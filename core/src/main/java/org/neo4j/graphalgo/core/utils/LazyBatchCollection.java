package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.AbstractIterator;

import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public final class LazyBatchCollection<T> extends AbstractCollection<T> {

    public interface BatchSupplier<T> {
        T newBatch(long start, long length);
    }

    public static <T> Collection<T> of(
            long nodeCount,
            long batchSize,
            BatchSupplier<T> supplier) {
        return new LazyBatchCollection<>(batchSize, nodeCount, false, null, supplier);
    }

    public static <T> Collection<T> ofCached(
            long nodeCount,
            long batchSize,
            Class<? extends T> kls,
            BatchSupplier<T> supplier) {
        return new LazyBatchCollection<>(batchSize, nodeCount, true, kls, supplier);
    }

    private final boolean saveResults;
    private final Class<? extends T> kls;
    private final BatchSupplier<T> supplier;
    private final long nodeCount;
    private final long batchSize;
    private final int numberOfBatches;

    private T[] batches;

    private LazyBatchCollection(
            long batchSize,
            long nodeCount,
            boolean saveResults,
            Class<? extends T> kls,
            BatchSupplier<T> supplier) {
        this.saveResults = saveResults;
        this.kls = kls;
        this.supplier = supplier;
        this.nodeCount = nodeCount;
        this.batchSize = batchSize;
        numberOfBatches = Math.toIntExact(ParallelUtil.threadSize(batchSize, nodeCount));
    }

    @Override
    public final Iterator<T> iterator() {
        if (batches != null) {
            return Arrays.asList(batches).iterator();
        }
        if (saveResults) {
            //noinspection unchecked
            batches = (T[]) Array.newInstance(kls, numberOfBatches);
        }
        return new AbstractIterator<T>() {
            private int i;
            private long start;

            @Override
            protected T fetch() {
                int i = this.i++;
                if (i >= numberOfBatches) {
                    return done();
                }
                long start = this.start;
                this.start += batchSize;
                long length = Math.min(batchSize, nodeCount - start);
                T t = supplier.newBatch(start, length);
                if (batches != null) {
                    batches[i] = t;
                }
                return t;
            }
        };
    }

    @Override
    public final int size() {
        return numberOfBatches;
    }

    @Override
    public Object[] toArray() {
        if (batches != null) {
            return batches;
        }
        return super.toArray();
    }
}
