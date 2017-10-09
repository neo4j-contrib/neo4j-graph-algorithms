package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;


public final class TrackingLongDoubleHashMap extends LongDoubleHashMap {
    private final AllocationTracker tracker;

    public TrackingLongDoubleHashMap(AllocationTracker tracker) {
        super(DEFAULT_EXPECTED_ELEMENTS, DEFAULT_LOAD_FACTOR, HashOrderMixing.defaultStrategy());
        this.tracker = tracker;
        trackBuffers(keys.length);
    }

    @Override
    protected void allocateBuffers(final int arraySize) {
        // also during super class init where tracker is not yet initialized
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
        tracker.add(sizeOfLongArray(length));
        tracker.add(sizeOfDoubleArray(length));
    }
}
