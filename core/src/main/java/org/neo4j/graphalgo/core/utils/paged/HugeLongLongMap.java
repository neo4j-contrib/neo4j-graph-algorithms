package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.AbstractIterator;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.Iterator;

public final class HugeLongLongMap extends PagedDataStructure<LongLongMap> implements Iterable<LongLongCursor> {

    public static HugeLongLongMap newMap(long size) {
        return new HugeLongLongMap(size);
    }

    private HugeLongLongMap(long size) {
        super(size, RamUsageEstimator.NUM_BYTES_OBJECT_REF, LongLongMap.class);
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
    protected LongLongMap newPage() {
        return new LongLongHashMap(pageSize);
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
