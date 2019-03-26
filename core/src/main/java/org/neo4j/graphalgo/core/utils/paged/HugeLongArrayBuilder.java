package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;

public final class HugeLongArrayBuilder {

    private final HugeLongArray array;
    private final long numberOfNodes;
    private final AtomicLong allocationIndex;
    private final ThreadLocal<BulkAdder> adders;

    public static HugeLongArrayBuilder of(long numberOfNodes, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(numberOfNodes, tracker);
        return new HugeLongArrayBuilder(array, numberOfNodes);
    }

    private HugeLongArrayBuilder(HugeLongArray array, final long numberOfNodes) {
        this.array = array;
        this.numberOfNodes = numberOfNodes;
        this.allocationIndex = new AtomicLong();
        this.adders = ThreadLocal.withInitial(this::newBulkAdder);
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder(array, array.newCursor());
    }

    public BulkAdder allocate(final long nodes) {
        long startIndex = allocationIndex.getAndAccumulate(nodes, this::upperAllocation);
        if (startIndex == numberOfNodes) {
            return null;
        }
        BulkAdder adder = adders.get();
        adder.reset(startIndex, upperAllocation(startIndex, nodes));
        return adder;
    }

    private long upperAllocation(long lower, long nodes) {
        return Math.min(numberOfNodes, lower + nodes);
    }

    public void set(long index, long value) {
        array.set(index, value);
    }

    public HugeLongArray build() {
        return array;
    }

    public long size() {
        return allocationIndex.get();
    }

    public final class BulkAdder {
        public long[] buffer;
        public int offset;
        public int length;
        public long start;
        private final HugeLongArray array;
        private final HugeLongArray.Cursor cursor;

        private BulkAdder(
                HugeLongArray array,
                HugeLongArray.Cursor cursor) {
            this.array = array;
            this.cursor = cursor;
        }

        private void reset(long start, long end) {
            array.cursor(this.cursor, start, end);
            this.start = start;
            buffer = null;
            offset = 0;
            length = 0;
        }

        public boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }

        public boolean add(long value) {
            while (true) {
                if (length > 0) {
                    buffer[offset++] = value;
                    --length;
                    return true;
                }
                if (!nextBuffer()) {
                    return false;
                }
            }
        }
    }
}
