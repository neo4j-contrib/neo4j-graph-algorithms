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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

class HugeAdjacencyBuilder {

    private final HugeAdjacencyListBuilder adjacency;

    private HugeAdjacencyListBuilder.Allocator allocator;
    private AdjacencyCompression compression;

    private final AllocationTracker tracker;

    HugeAdjacencyBuilder(AllocationTracker tracker) {
        adjacency = HugeAdjacencyListBuilder.newBuilder(tracker);
        this.tracker = tracker;
    }

    HugeAdjacencyBuilder(
            HugeAdjacencyListBuilder adjacency,
            HugeAdjacencyListBuilder.Allocator allocator,
            AdjacencyCompression compression,
            AllocationTracker tracker) {
        this.adjacency = adjacency;
        this.allocator = allocator;
        this.compression = compression;
        this.tracker = tracker;
    }

    final HugeAdjacencyBuilder threadLocalCopy() {
        return new HugeAdjacencyBuilder(
                adjacency,
                adjacency.newAllocator(),
                new AdjacencyCompression(),
                tracker);
    }

    final void prepare() {
        allocator.prepare();
    }

    final long applyVariableDeltaEncoding(long[] targets, int length) {
        compression.copyFrom(targets, length);
        int requiredBytes = compression.applyDeltaEncodingAndCalculateRequiredBytes();
        long address = allocator.allocate(4 + requiredBytes);
        int offset = compression.writeDegree(allocator.page, allocator.offset);
        offset = compression.compress(allocator.page, offset);
        allocator.offset = offset;
        return address;
    }

    static HugeGraph apply(
            final AllocationTracker tracker,
            final HugeIdMap idMapping,
            final HugeWeightMapping weights,
            final HugeAdjacencyBuilder inAdjacency,
            final HugeAdjacencyBuilder outAdjacency,
            final HugeLongArray inOffsets,
            final HugeLongArray outOffsets) {

        HugeAdjacencyList outAdjacencyList = null;
        if (outAdjacency != null) {
            outAdjacencyList = outAdjacency.adjacency.build();
        }
        HugeAdjacencyList inAdjacencyList = null;
        if (inAdjacency != null) {
            inAdjacencyList = inAdjacency.adjacency.build();
        }

        return new HugeGraphImpl(
                tracker, idMapping, weights,
                inAdjacencyList, outAdjacencyList, inOffsets, outOffsets
        );
    }
}
