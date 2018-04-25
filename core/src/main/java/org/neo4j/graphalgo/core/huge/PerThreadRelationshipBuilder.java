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

import org.neo4j.graphalgo.core.huge.HugeAdjacencyListBuilder.Allocator;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RenamesCurrentThread;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.helpers.Exceptions;

import java.util.concurrent.ArrayBlockingQueue;

final class PerThreadRelationshipBuilder implements RenamesCurrentThread, Runnable {

    private static final int INIT_OUT = 1;
    private static final int INIT_IN = 2;
    private static final int QUEUE_DONE = 4;

    private final int threadIndex;
    private final long startId;
    private final int numberOfElements;
    private final ArrayBlockingQueue<RelationshipsBatch> queue;
    private final ImportProgress progress;
    private final AllocationTracker tracker;

    private int initStatus;

    private int[] outDegrees;
    private CompressedLongArray[] outTargets;
    private final HugeLongArray outOffsets;
    private final Allocator outAllocator;
    private final AdjacencyCompression inCompression;

    private int[] inDegrees;
    private CompressedLongArray[] inTargets;
    private final HugeLongArray inOffsets;
    private final Allocator inAllocator;
    private final AdjacencyCompression outCompression;

    private long size;

    PerThreadRelationshipBuilder(
            ImportProgress progress,
            AllocationTracker tracker,
            int threadIndex,
            long startId,
            int numberOfElements,
            ArrayBlockingQueue<RelationshipsBatch> queue,
            int[] outDegrees,
            HugeLongArray outOffsets,
            HugeAdjacencyListBuilder outAdjacency,
            int[] inDegrees,
            HugeLongArray inOffsets,
            HugeAdjacencyListBuilder inAdjacency) {
        this.progress = progress;
        this.tracker = tracker;
        this.threadIndex = threadIndex;
        this.startId = startId;
        this.numberOfElements = numberOfElements;
        this.queue = queue;
        this.outOffsets = outOffsets;
        this.outDegrees = outDegrees;
        this.outAllocator = outAdjacency != null ? outAdjacency.newAllocator() : null;
        this.outCompression = outAdjacency != null ? new AdjacencyCompression() : null;
        this.inOffsets = inOffsets;
        this.inDegrees = inDegrees;
        this.inAllocator = inAdjacency != null ? inAdjacency.newAllocator() : null;
        this.inCompression = inAdjacency != null ? new AdjacencyCompression() : null;
    }

    @Override
    public void run() {
        Runnable revertName = RenamesCurrentThread.renameThread(threadName());
        try {
            runImport();
        } catch (Exception e) {
            drainQueue(e);
        } finally {
            revertName.run();
        }
    }

    private void runImport() {
        try {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        initStatus |= QUEUE_DONE;
                        break;
                    }
                    addRelationship(relationship);
                }
            }
            release(true);
        } catch (Exception e) {
            Exceptions.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private void release(boolean flushBuffers) {
        inDegrees = null;
        outDegrees = null;
        if (flushBuffers) {
            flush(outTargets, outOffsets, outAllocator, outCompression);
            flush(inTargets, inOffsets, inAllocator, inCompression);
        }
        outTargets = null;
        inTargets = null;
        if (outCompression != null) {
            outCompression.release();
        }
        if (inCompression != null) {
            inCompression.release();
        }
    }

    private void drainQueue(Exception e) {
        if ((initStatus & QUEUE_DONE) == 0) {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        break;
                    }
                }
            }
        }
        release(false);
        Exceptions.throwIfUnchecked(e);
        throw new RuntimeException(e);
    }

    @Override
    public String threadName() {
        return "HugeRelationshipImport-" + threadIndex;
    }

    private RelationshipsBatch pollNext() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RelationshipsBatch.SENTINEL;
        }
//        LoadedRelationship relationship = queue.poll();
//        while (relationship == null) {
//            Thread.yield();
//            relationship = queue.poll();
//        }
//        return relationship;
    }

    private void addRelationship(RelationshipsBatch batch) {
        switch (batch.direction) {
            case OUTGOING:
                initOut();
                addRelationship(batch, outTargets, outDegrees, outOffsets, outAllocator, outCompression);
                break;
            case INCOMING:
                initIn();
                addRelationship(batch, inTargets, inDegrees, inOffsets, inAllocator, inCompression);
                break;
            default:
                throw new IllegalArgumentException(batch.direction + " unsupported in loading");
        }
        progress.relationshipBatchImported(batch.length >> 1);
    }

    private void initOut() {
        if ((initStatus & INIT_OUT) == 0) {
            initStatus |= INIT_OUT;
            outTargets = new CompressedLongArray[numberOfElements];
            outAllocator.prepare();
        }
    }

    private void initIn() {
        if ((initStatus & INIT_IN) == 0) {
            initStatus |= INIT_IN;
            inTargets = new CompressedLongArray[numberOfElements];
            inAllocator.prepare();
        }
    }

    private void addRelationship(
            RelationshipsBatch batch, CompressedLongArray[] targets, int[] degrees,
            HugeLongArray offsets, Allocator allocator, AdjacencyCompression compression) {
        final long[] sourceAndTargets = batch.sourceAndTargets;
        final int length = batch.length;
        for (int i = 0; i < length; i += 2) {
            final long source = sourceAndTargets[i];
            final long target = sourceAndTargets[i + 1];
            addRelationship(source, target, targets, degrees, offsets, allocator, compression);
        }
    }

    private void addRelationship(
            long source, long target, CompressedLongArray[] targets, int[] degrees,
            HugeLongArray offsets, Allocator allocator, AdjacencyCompression compression) {
        int localFrom = (int) (source - startId);
        assert localFrom < numberOfElements;
        int degree = addTarget(localFrom, target, targets, degrees);
        if (degree >= degrees[localFrom]) {
            applyVariableDeltaEncoding(targets[localFrom], allocator, compression, source, offsets);
            targets[localFrom] = null;
        }
        ++size;
    }

    private int addTarget(int source, long targetId, CompressedLongArray[] targets, int[] degrees) {
        CompressedLongArray target = targets[source];
        if (target == null) {
            targets[source] = new CompressedLongArray(tracker, targetId, degrees[source]);
            return 1;
        }
        return target.add(targetId);
    }

    private void flush(
            CompressedLongArray[] targetIds,
            HugeLongArray offsets,
            Allocator allocator,
            AdjacencyCompression compression) {
        if (targetIds != null && offsets != null && allocator != null) {
            for (int i = 0, length = targetIds.length; i < length; i++) {
                CompressedLongArray targets = targetIds[i];
                if (targets != null) {
                    applyVariableDeltaEncoding(targets, allocator, compression, i + startId, offsets);
                    targetIds[i] = null;
                }
            }
        }
    }

    private void applyVariableDeltaEncoding(
            CompressedLongArray array,
            Allocator allocator,
            AdjacencyCompression compression,
            long sourceId,
            HugeLongArray offsets) {
        compression.copyFrom(array);
        compression.applyDeltaEncoding();
        long address = array.compress(compression, allocator);
        offsets.set(sourceId, address);
        array.release();
    }

    @Override
    public String toString() {
        return "PerThreadBuilder{" +
                "idBase=" + startId +
                ", maxId=" + (startId + (long) numberOfElements) +
                ", size=" + size +
                '}';
    }
}
