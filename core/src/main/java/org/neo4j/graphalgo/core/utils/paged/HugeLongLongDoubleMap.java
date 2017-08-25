package org.neo4j.graphalgo.core.utils.paged;

import org.apache.lucene.util.RamUsageEstimator;
import org.neo4j.graphalgo.core.utils.container.LongLongDoubleHashMap;


public final class HugeLongLongDoubleMap extends PagedDataStructure<LongLongDoubleHashMap> {

    public static HugeLongLongDoubleMap newMap(long size) {
        return new HugeLongLongDoubleMap(size);
    }

    private HugeLongLongDoubleMap(long size) {
        super(
                size,
                RamUsageEstimator.NUM_BYTES_OBJECT_REF,
                LongLongDoubleHashMap.class);
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

    @Override
    protected LongLongDoubleHashMap newPage() {
        return new LongLongDoubleHashMap(pageSize);
    }
}
