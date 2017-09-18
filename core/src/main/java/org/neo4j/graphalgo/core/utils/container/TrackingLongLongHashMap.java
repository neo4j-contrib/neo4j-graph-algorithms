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
