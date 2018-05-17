package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

final class HugeAdjacencyBuilder {

    private final HugeAdjacencyListBuilder adjacency;
    private final HugeLongArray offsets;

    private HugeAdjacencyListBuilder.Allocator allocator;
    private AdjacencyCompression compression;

    private final AllocationTracker tracker;

    HugeAdjacencyBuilder(long nodeCount, AllocationTracker tracker) {
        offsets = HugeLongArray.newArray(nodeCount, tracker);
        adjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        this.tracker = tracker;
    }

    private HugeAdjacencyBuilder(
            HugeAdjacencyListBuilder adjacency,
            HugeLongArray offsets,
            HugeAdjacencyListBuilder.Allocator allocator,
            AdjacencyCompression compression,
            AllocationTracker tracker) {
        this.adjacency = adjacency;
        this.offsets = offsets;
        this.allocator = allocator;
        this.compression = compression;
        this.tracker = tracker;
    }

    private HugeAdjacencyBuilder threadLocalCopy() {
        return new HugeAdjacencyBuilder(
                adjacency, offsets, adjacency.newAllocator(), new AdjacencyCompression(), tracker
        );
    }

    void prepare() {
        allocator.prepare();
    }

    void applyVariableDeltaEncoding(CompressedLongArray array, long sourceId) {
        compression.copyFrom(array);
        compression.applyDeltaEncoding();
        long address = array.compress(compression, allocator);
        offsets.set(sourceId, address);
        array.release();
    }

    void release() {
        compression.release();
    }

    static HugeAdjacencyBuilder threadLocal(HugeAdjacencyBuilder builder) {
        return builder != null ? builder.threadLocalCopy() : null;
    }

    static HugeGraph apply(
            final AllocationTracker tracker,
            final HugeIdMap idMapping,
            final HugeWeightMapping weights,
            final HugeAdjacencyBuilder inAdjacency,
            final HugeAdjacencyBuilder outAdjacency) {

        HugeAdjacencyList outAdjacencyList = null;
        HugeLongArray outOffsets = null;
        if (outAdjacency != null) {
            outAdjacencyList = outAdjacency.adjacency.build();
            outOffsets = outAdjacency.offsets;
        }
        HugeAdjacencyList inAdjacencyList = null;
        HugeLongArray inOffsets = null;
        if (inAdjacency != null) {
            inAdjacencyList = inAdjacency.adjacency.build();
            inOffsets = inAdjacency.offsets;
        }

        return new HugeGraphImpl(
                tracker, idMapping, weights,
                inAdjacencyList, outAdjacencyList, inOffsets, outOffsets
        );
    }
}
