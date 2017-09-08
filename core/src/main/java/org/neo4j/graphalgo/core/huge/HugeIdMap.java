package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeBatchNodeIterable;
import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.api.HugeNodeIterator;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.paged.LongArray;

import java.util.Collection;
import java.util.function.LongPredicate;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public final class HugeIdMap implements HugeIdMapping, HugeNodeIterator, HugeBatchNodeIterable {

    private long nextGraphId;
    private LongArray graphIds;
    private HugeLongLongMap nodeToGraphIds;
    private final AllocationTracker tracker;

    /**
     * initialize the map with maximum node capacity
     */
    public HugeIdMap(long capacity, AllocationTracker tracker) {
        nodeToGraphIds = HugeLongLongMap.newMap(capacity, tracker);
        this.tracker = tracker;
    }

    public long mapOrGet(long externalId) {
        long internalId = nodeToGraphIds.getOrDefault(externalId, -1L);
        if (internalId == -1L) {
            internalId = nextGraphId++;
            nodeToGraphIds.put(externalId, internalId);
        }
        return internalId;
    }

    public void add(long longValue) {
        long internalId = nextGraphId++;
        nodeToGraphIds.put(longValue, internalId);
    }

    public long get(long longValue) {
        return nodeToGraphIds.getOrDefault(longValue, -1);
    }

    public void buildMappedIds() {
        graphIds = LongArray.newArray(hugeNodeCount(), tracker);
        for (final LongLongCursor cursor : nodeToGraphIds) {
            graphIds.set(cursor.value, cursor.key);
        }
    }

    @Override
    public long toHugeMappedNodeId(long nodeId) {
        return mapOrGet(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return graphIds.get(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeToGraphIds.containsKey(nodeId);
    }

    @Override
    public long hugeNodeCount() {
        return nextGraphId;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final long count = hugeNodeCount();
        for (long i = 0; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveLongIterator hugeNodeIterator() {
        return new IdIterator(hugeNodeCount());
    }

    @Override
    public Collection<PrimitiveLongIterable> hugeBatchIterables(int batchSize) {
        return LazyBatchCollection.of(
                hugeNodeCount(),
                batchSize,
                IdIterable::new);
    }

    private static final class IdIterable implements PrimitiveLongIterable {
        private final long start;
        private final long length;

        private IdIterable(long start, long length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public PrimitiveLongIterator iterator() {
            return new IdIterator(start, length);
        }
    }

    private static final class IdIterator implements PrimitiveLongIterator {

        private long current;
        private long limit; // exclusive upper bound

        private IdIterator(long length) {
            this.current = 0;
            this.limit = length;
        }

        private IdIterator(long start, long length) {
            this.current = start;
            this.limit = start + length;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public long next() {
            return current++;
        }
    }
}
