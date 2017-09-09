package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.container.LongLongDoubleHashMap;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_DOUBLE;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_LONG;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;


public final class HugeLongLongDoubleMap extends PagedDataStructure<LongLongDoubleHashMap> {


    private static final PageAllocator.Factory<LongLongDoubleHashMap> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(NUM_BYTES_OBJECT_REF);
        long bufferLength = (long) BitUtil.nextHighestPowerOfTwo(pageSize) << 1;
        long keyUsage = alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_LONG * bufferLength);
        long valueUsage = alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_DOUBLE * bufferLength);

        long pageUsage = shallowSizeOfInstance(LongLongDoubleHashMap.class);
        pageUsage += keyUsage; // keys1
        pageUsage += keyUsage; // keys2
        pageUsage += valueUsage; // values

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new LongLongDoubleHashMap(pageSize),
                new LongLongDoubleHashMap[0]);
    }

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, HugeLongLongDoubleMap.class);
    }

    public static HugeLongLongDoubleMap newMap(long size, AllocationTracker tracker) {
        return new HugeLongLongDoubleMap(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private HugeLongLongDoubleMap(long size, PageAllocator<LongLongDoubleHashMap> allocator) {
        super(size, allocator);
    }

    public double getOrDefault(long index1, long index2, double defaultValue) {
        assert index1 < capacity();
        final int pageIndex = pageIndex(index1);
        return pages[pageIndex].getOrDefault(index1, index2, defaultValue);
    }

    public double put(long index1, long index2, double value) {
        assert index1 < capacity();
        final int pageIndex = pageIndex(index1);
        return pages[pageIndex].put(index1, index2, value);
    }
}
