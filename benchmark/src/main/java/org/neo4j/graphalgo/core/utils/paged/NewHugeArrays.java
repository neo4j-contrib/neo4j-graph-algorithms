package org.neo4j.graphalgo.core.utils.paged;

public final class NewHugeArrays {
    public static HugeLongArray newPagedArray(long size, AllocationTracker tracker) {
        return HugeLongArray.newPagedArray(size, tracker);
    }
}
