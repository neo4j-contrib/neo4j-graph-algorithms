package org.neo4j.graphalgo.core.huge;

class LongsBuffer {

    @FunctionalInterface
    public interface BucketConsumer {
        void apply(int bucketIndex, long[] bucket, int bucketLength) throws InterruptedException;
    }

    private long[][] targets;
    private int[] lengths;

    LongsBuffer(int numBuckets, int batchSize) {
        if (numBuckets > 0) {
            targets = new long[numBuckets][batchSize];
            lengths = new int[numBuckets];
        }
    }

    int addRelationship(int bucketIndex, long source, long target) {
        int len = lengths[bucketIndex] += 2;
        long[] sourceAndTargets = targets[bucketIndex];
        sourceAndTargets[len - 2] = source;
        sourceAndTargets[len - 1] = target;
        return len;
    }


    long[] get(int bucketIndex) {
        return targets[bucketIndex];
    }

    void reset(final int bucketIndex, final long[] newBuffer) {
        targets[bucketIndex] = newBuffer;
        lengths[bucketIndex] = 0;
    }

    void drainAndRelease(BucketConsumer consumer) throws InterruptedException {
        long[][] targets = this.targets;
        int[] lengths = this.lengths;
        int length = targets.length;
        for (int i = 0; i < length; i++) {
            consumer.apply(i, targets[i], lengths[i]);
            targets[i] = null;
        }
        this.targets = null;
        this.lengths = null;
    }

    static LongsBuffer EMPTY = new EmptyLongsBuffer();
    private static long[] EMPTY_LONGS = new long[0];

    private static final class EmptyLongsBuffer extends LongsBuffer {
        EmptyLongsBuffer() {
            super(0, 0);
        }

        @Override
        int addRelationship(final int bucketIndex, final long source, final long target) {
            return 0;
        }

        @Override
        long[] get(final int bucketIndex) {
            return EMPTY_LONGS;
        }

        @Override
        void reset(final int bucketIndex, final long[] newBuffer) {
        }

        @Override
        void drainAndRelease(final BucketConsumer consumer) {
        }
    }
}
