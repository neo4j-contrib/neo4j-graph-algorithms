package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

final class HugeAdjacencyBuilder {

    private final HugeAdjacencyListBuilder adjacency;

    private HugeAdjacencyListBuilder.Allocator allocator;
    private AdjacencyCompression compression;
    private HugeAdjacencyOffsets globalOffsets;
    private long[] offsets;

    private final AllocationTracker tracker;

    HugeAdjacencyBuilder(AllocationTracker tracker) {
        adjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        this.tracker = tracker;
    }

    private HugeAdjacencyBuilder(
            HugeAdjacencyListBuilder adjacency,
            HugeAdjacencyListBuilder.Allocator allocator,
            AdjacencyCompression compression,
            long[] offsets,
            AllocationTracker tracker) {
        this.adjacency = adjacency;
        this.allocator = allocator;
        this.compression = compression;
        this.offsets = offsets;
        this.tracker = tracker;
    }

    HugeAdjacencyBuilder threadLocalCopy(long[] offsets) {
        return new HugeAdjacencyBuilder(
                adjacency, adjacency.newAllocator(), new AdjacencyCompression(), offsets, tracker
        );
    }

    void prepare() {
        allocator.prepare();
    }

    void setGlobalOffsets(HugeAdjacencyOffsets globalOffsets) {
        this.globalOffsets = globalOffsets;
    }

    int degree(int localId) {
        return (int) offsets[localId];
    }

    void applyVariableDeltaEncoding(CompressedLongArray array, int localId) {
        compression.copyFrom(array);
        compression.applyDeltaEncoding();
        long address = array.compress(compression, allocator);
        offsets[localId] = address;
        array.release();
    }

    void release() {
        compression.release();
    }

    static HugeAdjacencyBuilder threadLocal(HugeAdjacencyBuilder builder, long[] offsets) {
        return builder != null ? builder.threadLocalCopy(offsets) : null;
    }

    static HugeGraph apply(
            final AllocationTracker tracker,
            final HugeIdMap idMapping,
            final HugeWeightMapping weights,
            final HugeAdjacencyBuilder inAdjacency,
            final HugeAdjacencyBuilder outAdjacency) {

        HugeAdjacencyList outAdjacencyList = null;
        HugeAdjacencyOffsets outOffsets = null;
        if (outAdjacency != null) {
            outAdjacencyList = outAdjacency.adjacency.build();
            outOffsets = outAdjacency.globalOffsets;
        }
        HugeAdjacencyList inAdjacencyList = null;
        HugeAdjacencyOffsets inOffsets = null;
        if (inAdjacency != null) {
            inAdjacencyList = inAdjacency.adjacency.build();
            inOffsets = inAdjacency.globalOffsets;
        }

        return new HugeGraphImpl(
                tracker, idMapping, weights,
                inAdjacencyList, outAdjacencyList, inOffsets, outOffsets
        );
    }
}
