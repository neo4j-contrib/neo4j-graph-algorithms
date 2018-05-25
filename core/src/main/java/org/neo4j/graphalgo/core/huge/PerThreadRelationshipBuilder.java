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

import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.BlockingQueue;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;


final class PerThreadRelationshipBuilder extends StatementAction {

    private static final int INIT_OUT = 1;
    private static final int INIT_IN = 2;
    private static final int QUEUE_DONE = 4;

    private final int threadIndex;
    private final long startId;
    private final int numberOfElements;
    private final BlockingQueue<RelationshipsBatch> queue;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeWeightMapBuilder weights;

    private Read read;
    private CursorFactory cursors;

    private final HugeAdjacencyBuilder outAdjacency;
    private CompressedLongArray[] outTargets;

    private final HugeAdjacencyBuilder inAdjacency;
    private CompressedLongArray[] inTargets;

    private int initStatus;

    PerThreadRelationshipBuilder(
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeWeightMapBuilder weights,
            BlockingQueue<RelationshipsBatch> queue,
            int threadIndex,
            long startId,
            int numberOfElements,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency) {
        super(api);
        this.progress = progress;
        this.tracker = tracker;
        this.weights = weights;
        this.queue = queue;
        this.threadIndex = threadIndex;
        this.startId = startId;
        this.numberOfElements = numberOfElements;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        try {
            useKernelTransaction(transaction);
            runImport();
        } catch (Exception e) {
            drainQueue(e);
        } finally {
            unsetKernelTransaction();
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
                    addRelationshipsBatch(relationship);
                }
            }
            release(true);
        } catch (Exception e) {
            Exceptions.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    void useKernelTransaction(KernelTransaction transaction) {
        read = transaction.dataRead();
        cursors = transaction.cursors();
    }

    private void unsetKernelTransaction() {
        read = null;
        cursors = null;
    }

    void pushBatch(RelationshipsBatch relationship) {
        if (relationship == RelationshipsBatch.SENTINEL) {
            initStatus |= QUEUE_DONE;
            release(true);
            return;
        }
        try (RelationshipsBatch batch = relationship) {
            addRelationshipsBatch(batch);
        }
    }

    private void release(boolean flushBuffers) {
        if (flushBuffers) {
            flush(outTargets, outAdjacency);
            flush(inTargets, inAdjacency);
        }
        if (outTargets != null) {
            tracker.remove(sizeOfObjectArray(outTargets.length));
            outTargets = null;
        }
        if (inTargets != null) {
            tracker.remove(sizeOfObjectArray(inTargets.length));
            inTargets = null;
        }
        if (outAdjacency != null) {
            outAdjacency.release();
        }
        if (inAdjacency != null) {
            inAdjacency.release();
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
    }

    private void addRelationshipsBatch(RelationshipsBatch batch) {
        switch (batch.dataFlag()) {
            case RelationshipsBatch.JUST_RELATIONSHIPS:
                addRelationship(batch);
                break;
            case RelationshipsBatch.JUST_WEIGHTS:
                addWeight(batch);
                break;
            case RelationshipsBatch.RELS_AND_WEIGHTS:
                addRelationshipAndWeight(batch);
                break;
            default:
                throw new IllegalArgumentException("unsupported");
        }
    }

    private void addRelationshipAndWeight(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        final CompressedLongArray[] targets;
        final HugeAdjacencyBuilder adjacency;
        if (batch.isOut()) {
            initOut();
            targets = outTargets;
            adjacency = outAdjacency;
        } else {
            initIn();
            targets = inTargets;
            adjacency = inAdjacency;
        }

        for (int i = 0; i < length; i += 3) {
            final long source = sourceTargetIds[i];
            final long target = sourceTargetIds[1 + i];
            final long relId = sourceTargetIds[2 + i];
            addRelationshipAndWeight(source, target, relId, targets, adjacency);
        }
        progress.relationshipsImported(batch.length / 3);
    }

    private void addRelationship(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        final CompressedLongArray[] targets;
        final HugeAdjacencyBuilder adjacency;
        if (batch.isOut()) {
            initOut();
            targets = outTargets;
            adjacency = outAdjacency;
        } else {
            initIn();
            targets = inTargets;
            adjacency = inAdjacency;
        }

        for (int i = 0; i < length; i += 2) {
            final long source = sourceTargetIds[i];
            final long target = sourceTargetIds[1 + i];
            addRelationship(source, target, targets, adjacency);
        }
        progress.relationshipsImported(batch.length >> 1);
    }

    private void addWeight(RelationshipsBatch batch) {
        final long[] sourceTargetIds = batch.sourceTargetIds;
        final int length = batch.length;
        for (int i = 0; i < length; i += 3) {
            final long source = sourceTargetIds[i];
            final long target = sourceTargetIds[1 + i];
            final long relId = sourceTargetIds[2 + i];
            addWeight(source, target, relId);
        }
    }

    private void initOut() {
        if ((initStatus & INIT_OUT) == 0) {
            initStatus |= INIT_OUT;
            outTargets = new CompressedLongArray[numberOfElements];
            tracker.add(sizeOfObjectArray(numberOfElements));
            outAdjacency.prepare();
        }
    }

    private void initIn() {
        if ((initStatus & INIT_IN) == 0) {
            initStatus |= INIT_IN;
            inTargets = new CompressedLongArray[numberOfElements];
            tracker.add(sizeOfObjectArray(numberOfElements));
            inAdjacency.prepare();
        }
    }

    private void addRelationshipAndWeight(
            long source, long target, long relId,
            CompressedLongArray[] targets, HugeAdjacencyBuilder adjacency) {
        int localFrom = (int) (source - startId);
        assert localFrom < numberOfElements;
        int endDegree = adjacency.degree(localFrom);
        int degree = addTarget(localFrom, target, targets, endDegree);
        weights.load(relId, target, localFrom, cursors, read);
        if (degree >= endDegree) {
            adjacency.applyVariableDeltaEncoding(targets[localFrom], localFrom);
            targets[localFrom] = null;
        }
    }

    private void addRelationship(
            long source, long target,
            CompressedLongArray[] targets,
            HugeAdjacencyBuilder adjacency) {
        int localFrom = (int) (source - startId);
        assert localFrom < numberOfElements;
        int endDegree = adjacency.degree(localFrom);
        int degree = addTarget(localFrom, target, targets, endDegree);
        if (degree >= endDegree) {
            adjacency.applyVariableDeltaEncoding(targets[localFrom], localFrom);
            targets[localFrom] = null;
        }
    }

    private void addWeight(long source, long target, long relId) {
        int localFrom = (int) (source - startId);
        assert localFrom < numberOfElements;
        weights.load(relId, target, localFrom, cursors, read);
    }

    private int addTarget(int source, long targetId, CompressedLongArray[] targets, int degree) {
        CompressedLongArray target = targets[source];
        if (target == null) {
            targets[source] = new CompressedLongArray(tracker, targetId, degree);
            return 1;
        }
        return target.add(targetId);
    }

    private void flush(CompressedLongArray[] targetIds, HugeAdjacencyBuilder adjacency) {
        if (targetIds != null && adjacency != null) {
            for (int i = 0, length = targetIds.length; i < length; i++) {
                CompressedLongArray targets = targetIds[i];
                if (targets != null) {
                    adjacency.applyVariableDeltaEncoding(targets, i);
                    targetIds[i] = null;
                }
            }
        }
    }

    @Override
    public String toString() {
        return "PerThreadBuilder{" +
                "idBase=" + startId +
                ", maxId=" + (startId + (long) numberOfElements) +
                '}';
    }
}
