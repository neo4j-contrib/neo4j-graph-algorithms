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
        if (targets != null) {
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
    }
}
