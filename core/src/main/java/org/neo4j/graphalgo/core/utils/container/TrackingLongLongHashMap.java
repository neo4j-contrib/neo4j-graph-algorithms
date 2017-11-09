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
package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongLongHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;


public final class TrackingLongLongHashMap extends LongLongHashMap {
    private final AllocationTracker tracker;

    public TrackingLongLongHashMap(AllocationTracker tracker) {
        super(DEFAULT_EXPECTED_ELEMENTS, DEFAULT_LOAD_FACTOR, HashOrderMixing.defaultStrategy());
        this.tracker = tracker;
        trackBuffers(keys.length);
    }

    @Override
    protected void allocateBuffers(final int arraySize) {
        // also during super class init
        if (!AllocationTracker.isTracking(tracker)) {
            super.allocateBuffers(arraySize);
            return;
        }
        int lengthBefore = keys.length;
        super.allocateBuffers(arraySize);
        int lengthAfter = keys.length;
        trackBuffers(-lengthBefore);
        trackBuffers(lengthAfter);
    }

    private void trackBuffers(int length) {
        // << 1 to multiply by two since we have two buffer arrays
        tracker.add(sizeOfLongArray(length) << 1);
    }
}
