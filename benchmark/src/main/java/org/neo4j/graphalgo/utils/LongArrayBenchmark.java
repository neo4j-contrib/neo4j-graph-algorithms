package org.neo4j.graphalgo.utils;

import org.neo4j.graphalgo.core.utils.paged.LongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.DynamicLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapLongArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LongArrayBenchmark {

    @Benchmark
    public long _01_primitive_get(LongArrays arrays) {
        final int size = arrays.size;
        final long[] array = arrays.primitive;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array[i];
        }
        return res;
    }

    @Benchmark
    public long[] _02_primitive_set(LongArrays arrays) {
        final int size = arrays.size;
        final long[] array = arrays.primitive;
        long[] result = new long[size];
        for (int i = 0; i < size; i++) {
            if (array[i] >= 0) {
                result[i] = array[i];
            }
        }
        return result;
    }

    @Benchmark
    public long _03_paged_get(LongArrays arrays) {
        final int size = arrays.size;
        final LongArray array = arrays.paged;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public LongArray _04_paged_set(LongArrays arrays) {
        return LongArrays.createPaged(arrays.primitive);
    }

    @Benchmark
    public long _05_sparse_get(LongArrays arrays) {
        final int size = arrays.size;
        final SparseLongArray array = arrays.sparse;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public SparseLongArray _06_sparse_set(LongArrays arrays) {
        return LongArrays.createSparse(arrays.primitive);
    }

    @Benchmark
    public long _07_offHeap_get(LongArrays arrays) {
        final int size = arrays.size;
        final OffHeapLongArray array = arrays.offHeap;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public OffHeapLongArray _08_offHeap_set(LongArrays arrays) {
        return LongArrays.createOffHeap(arrays.primitive);
    }

    @Benchmark
    public long _09_chunked_get(LongArrays arrays) {
        final int size = arrays.size;
        final DynamicLongArray array = arrays.chunked;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public DynamicLongArray _10_chunked_set(LongArrays arrays) {
        return LongArrays.createChunked(arrays.primitive);
    }
}
