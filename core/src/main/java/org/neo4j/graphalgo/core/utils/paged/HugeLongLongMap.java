package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.AbstractIterator;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_LONG;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;

public final class HugeLongLongMap extends PagedDataStructure<LongLongMap> implements Iterable<LongLongCursor> {

    private static final PageAllocator.Factory<LongLongMap> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        long bufferLength = (long) Math.ceil(pageSize / 0.75f);
        bufferLength = BitUtil.nextHighestPowerOfTwo(bufferLength);
        long bufferUsage = alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_LONG * bufferLength);

        long pageUsage = RamUsageEstimator.shallowSizeOfInstance(LongLongHashMap.class);
        pageUsage += bufferUsage; // keys
        pageUsage += bufferUsage; // values

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new LongLongHashMap(pageSize),
                new LongLongHashMap[0]);
    }

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, HugeLongLongMap.class);
    }

    public static HugeLongLongMap newMap(long size, AllocationTracker tracker) {
        return new HugeLongLongMap(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private HugeLongLongMap(long size, PageAllocator<LongLongMap> allocator) {
        super(size, allocator);
    }

    public long getOrDefault(long index, long defaultValue) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        return pages[pageIndex].getOrDefault(index, defaultValue);
    }

    public long put(long index, long value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final LongLongMap page = pages[pageIndex];
        return page.put(index, value);
    }

    public boolean containsKey(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final LongLongMap page = pages[pageIndex];
        return page.containsKey(index);
    }

    @Override
    public Iterator<LongLongCursor> iterator() {
        return new Iter();
    }

    private final class Iter extends AbstractIterator<LongLongCursor> {

        private final Iterator<LongLongMap> maps;
        private Iterator<LongLongCursor> current;

        private Iter() {
            maps = Arrays.asList(pages).iterator();
            current = maps.next().iterator();
        }

        @Override
        protected LongLongCursor fetch() {
            while (!current.hasNext()) {
                if (!maps.hasNext()) {
                    return done();
                }
                current = maps.next().iterator();
            }
            return current.next();
        }
    }
}
