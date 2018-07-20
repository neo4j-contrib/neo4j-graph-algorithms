/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeWeightMapping;

import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.neo4j.graphalgo.core.huge.AdjacencyCompression.applyDeltaEncodingAndCalculateRequiredBytes;
import static org.neo4j.graphalgo.core.huge.AdjacencyCompression.writeDegree;

class HugeAdjacencyBuilder {

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

    HugeAdjacencyBuilder(
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

    final HugeAdjacencyBuilder threadLocalCopy(long[] offsets, boolean loadDegrees) {
        if (loadDegrees) {
            return new HugeAdjacencyBuilder(
                    adjacency,
                    adjacency.newAllocator(),
                    new AdjacencyCompression(),
                    offsets,
                    tracker);
        }
        return new NoDegreeHAB(adjacency, adjacency.newAllocator(), new AdjacencyCompression(), offsets, tracker);
    }

    final void prepare() {
        allocator.prepare();
    }

    final void setGlobalOffsets(HugeAdjacencyOffsets globalOffsets) {
        this.globalOffsets = globalOffsets;
    }

    final int applyVariableDeltaEncoding(long[] targets, int length, int localId) {
        long requiredBytesAndLength = applyDeltaEncodingAndCalculateRequiredBytes(targets, length);
        int degree = RawValues.getHead(requiredBytesAndLength);
        int requiredBytes = RawValues.getTail(requiredBytesAndLength);
        final long address = allocateIds(targets, requiredBytes, degree);
        offsets[localId] = address;
        return degree;
    }

    final void applyVariableDeltaEncoding(CompressedLongArray array, int localId) {
        compression.copyFrom(array);
        compression.applyDeltaEncoding();
        long address = array.compress(compression, allocator);
        offsets[localId] = address;
        array.release();
    }

    private synchronized long allocateIds(long[] targets, int requiredBytes, int degree) {
        // sizeOf(degree) + compression bytes
        long address = allocator.allocate(4 + requiredBytes);
        int offset = allocator.offset;
        offset = writeDegree(allocator.page, offset, degree);
        offset = AdjacencyCompression.compress(targets, degree, allocator.page, offset);
        allocator.offset = offset;
        return address;
    }

    int degree(int localId) {
        return (int) offsets[localId];
    }

    final void release() {
        compression.release();
    }

    static HugeAdjacencyBuilder threadLocal(HugeAdjacencyBuilder builder, long[] offsets, boolean loadDegrees) {
        return builder != null ? builder.threadLocalCopy(offsets, loadDegrees) : null;
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

    private static final class NoDegreeHAB extends HugeAdjacencyBuilder {
        private NoDegreeHAB(
                HugeAdjacencyListBuilder adjacency,
                HugeAdjacencyListBuilder.Allocator allocator,
                AdjacencyCompression compression,
                long[] offsets,
                AllocationTracker tracker) {
            super(adjacency, allocator, compression, offsets, tracker);
        }

        @Override
        int degree(final int localId) {
            return Integer.MAX_VALUE;
        }
    }
}
