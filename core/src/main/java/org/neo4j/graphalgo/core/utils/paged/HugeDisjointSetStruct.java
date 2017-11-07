package org.neo4j.graphalgo.core.utils.paged;


import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.api.HugeIdMapping;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class HugeDisjointSetStruct {

    private final LongArray parent;
    private final LongArray depth;
    private final long capacity;


    public HugeDisjointSetStruct(long capacity, AllocationTracker tracker) {
        parent = LongArray.newArray(capacity, tracker);
        depth = LongArray.newArray(capacity, tracker);
        this.capacity = capacity;
    }

    public HugeDisjointSetStruct reset() {
        parent.fill(-1);
        return this;
    }

    public static long estimateSize(long capacity) {
        return LongArray.estimateMemoryUsage(capacity) * 2
                + MemoryUsage.shallowSizeOfInstance(HugeDisjointSetStruct.class);
    }

    public boolean connected(long p, long q) {
        return find(p) == find(q);
    }

    public long find(long p) {
        return findPC(p);
    }

    private long findPC(long p) {
        long pv = parent.get(p);
        if (pv == -1L) {
            return p;
        }
        // path compression optimization
        // TODO
        long value = find(pv);
        parent.set(p, value);
        return value;
    }

    public void union(long p, long q) {
        final long pSet = find(p);
        final long qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        long dq = depth.get(qSet);
        long dp = depth.get(pSet);
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent.set(pSet, qSet);
        } else if (dp > dq) {
            parent.set(qSet, pSet);
        } else {
            parent.set(qSet, pSet);
            depth.addTo(pSet, depth.get(qSet) + 1);
        }
    }

    public HugeDisjointSetStruct merge(HugeDisjointSetStruct other) {

        if (other.capacity != this.capacity) {
            throw new IllegalArgumentException("Different Capacity");
        }

        LongArray.Cursor others = other.parent.cursor(0, other.parent.newCursor());
        long i = 0L;
        while (others.next()) {
            long[] array = others.array;
            int offset = others.offset;
            int limit = others.limit;
            while (offset < limit) {
                if (array[offset++] != -1L) {
                    union(i, other.find(i));
                }
                ++i;
            }
        }

        return this;
    }

    public long findNoOpt(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1) {
            p = np;
        }
        return p;
    }

    public int getSetCount() {
        LongScatterSet set = new LongScatterSet();
        for (long i = 0L; i < capacity; ++i) {
            long setId = find(i);
            set.add(setId);
        }
        return set.size();
    }

    public Stream<DisjointSetStruct.Result> resultStream(HugeIdMapping idMapping) {

        return LongStream.range(HugeIdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new DisjointSetStruct.Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }
}
