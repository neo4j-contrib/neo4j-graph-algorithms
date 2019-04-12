package org.neo4j.graphalgo.core.utils.paged;

public final class HugeLongArrayBuilder extends HugeArrayBuilder<long[], HugeLongArray> {

    public static HugeLongArrayBuilder of(long length, AllocationTracker tracker) {
        HugeLongArray array = HugeLongArray.newArray(length, tracker);
        return new HugeLongArrayBuilder(array, length);
    }

    private HugeLongArrayBuilder(HugeLongArray array, final long length) {
        super(array, length);
    }
}
