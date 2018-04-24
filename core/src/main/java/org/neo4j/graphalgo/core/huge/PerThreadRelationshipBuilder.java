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

import org.neo4j.graphalgo.core.loading.LoadRelationships;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import static org.neo4j.graphalgo.core.utils.paged.DeltaEncoding.vSize;

final class PerThreadRelationshipBuilder extends StatementAction {

    interface DegreeLoader {
        void load(NodeCursor cursor, LoadRelationships loader, int localId, int[] outDegrees, int[] inDegrees);
    }

    private static final int INIT_OUT = 1;
    private static final int INIT_IN = 2;
    private final int labelId;
    private final int[] relationshipTypeId;
    private final boolean loadAsUndirected;
    private final int threadIndex;
    private final long startId;
    private final int numberOfElements;
    private final ArrayBlockingQueue<RelationshipsBatch> queue;
    private final HugeIdMap idMap;
    private final ImportProgress progress;

    private long[] flushBuffer;
    private int initStatus;

    private int[] outDegrees;
    private CompressedLongArray[] outTargets;
    private final HugeLongArray outOffsets;
    private final ByteArray.LocalAllocator outAllocator;

    private int[] inDegrees;
    private CompressedLongArray[] inTargets;
    private final HugeLongArray inOffsets;
    private final ByteArray.LocalAllocator inAllocator;

    private long size;

    PerThreadRelationshipBuilder(
            GraphDatabaseAPI api,
            int labelId,
            int[] relationshipTypeId,
            boolean loadAsUndirected,
            ImportProgress progress,
            HugeIdMap idMap,
            int threadIndex,
            long startId,
            int numberOfElements,
            ArrayBlockingQueue<RelationshipsBatch> queue,
            HugeLongArray outOffsets,
            ByteArray outAdjacency,
            HugeLongArray inOffsets,
            ByteArray inAdjacency) {
        super(api);
        this.labelId = labelId;
        this.relationshipTypeId = relationshipTypeId;
        this.loadAsUndirected = loadAsUndirected;
        this.threadIndex = threadIndex;
        this.startId = startId;
        this.numberOfElements = numberOfElements;
        this.queue = queue;
        this.idMap = idMap;
        this.progress = progress;
        this.outOffsets = outOffsets;
        this.inOffsets = inOffsets;
        this.outAllocator = outAdjacency != null ? outAdjacency.newAllocator() : null;
        this.inAllocator = inAdjacency != null ? inAdjacency.newAllocator() : null;
        flushBuffer = new long[0];
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        loadDegrees(transaction);
        runImport();
    }

    private void loadDegrees(KernelTransaction transaction) {
        final DegreeLoader degree;
        final int[] inDegrees, outDegrees;
        if (loadAsUndirected) {
            degree = LoadDegreeUndirected.INSTANCE;
            outDegrees = new int[numberOfElements];
            inDegrees = null;
        } else {
            if (outAllocator != null) {
                if (inAllocator != null) {
                    degree = LoadDegreeBoth.INSTANCE;
                    outDegrees = new int[numberOfElements];
                    inDegrees = new int[numberOfElements];
                } else {
                    degree = LoadDegreeOut.INSTANCE;
                    outDegrees = new int[numberOfElements];
                    inDegrees = null;
                }
            } else {
                if (inAllocator != null) {
                    degree = LoadDegreeIn.INSTANCE;
                    outDegrees = null;
                    inDegrees = new int[numberOfElements];
                } else {
                    degree = LoadDegreeNone.INSTANCE;
                    outDegrees = null;
                    inDegrees = null;
                }
            }
        }

        loadDegrees(transaction, degree, inDegrees, outDegrees);

        this.outDegrees = outDegrees;
        this.inDegrees = inDegrees;
    }

    private void loadDegrees(
            KernelTransaction transaction,
            DegreeLoader degree,
            int[] inDegrees,
            int[] outDegrees) {
        final CursorFactory cursors = transaction.cursors();
        final LoadRelationships rels = LoadRelationships.of(cursors, relationshipTypeId);
        final Read read = transaction.dataRead();
        long startId = this.startId;
        long endId = startId + numberOfElements;
        if (labelId == Read.ANY_LABEL) {
            try (NodeCursor nodeCursor = cursors.allocateNodeCursor()) {
                read.allNodesScan(nodeCursor);
                while (nodeCursor.next()) {
                    long graphId = idMap.toHugeMappedNodeId(nodeCursor.nodeReference());
                    if (graphId >= startId && graphId < endId) {
                        int localId = (int) (graphId - startId);
                        degree.load(nodeCursor, rels, localId, outDegrees, inDegrees);
                    }
                }
            }
        } else {
            try (NodeLabelIndexCursor nodeCursor = cursors.allocateNodeLabelIndexCursor();
                 NodeCursor nc = cursors.allocateNodeCursor()) {
                read.nodeLabelScan(labelId, nodeCursor);
                while (nodeCursor.next()) {
                    long graphId = idMap.toHugeMappedNodeId(nodeCursor.nodeReference());
                    if (graphId >= startId && graphId < endId) {
                        int localId = (int) (graphId - startId);
                        nodeCursor.node(nc);
                        nc.next();
                        degree.load(nc, rels, localId, outDegrees, inDegrees);
                    }
                }
            }
        }
    }


    private void runImport() {
        try {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        break;
                    }
                    addRelationship(relationship);
                }
            }
//            if ((initStatus & INIT_OUT) == 0) {
//                tracker.remove(idMap.releaseOutDegrees());
//            }
//            if ((initStatus & INIT_IN) == 0) {
//                tracker.remove(idMap.releaseInDegrees());
//            }
            inDegrees = null;
            outDegrees = null;
            flush(outTargets, outOffsets, outAllocator);
            flush(inTargets, inOffsets, inAllocator);
            outTargets = null;
            inTargets = null;
        } catch (Exception e) {
            Exceptions.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
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
                addRelationship(batch, outTargets, outDegrees, outOffsets, outAllocator);
                break;
            case INCOMING:
                initIn();
                addRelationship(batch, inTargets, inDegrees, inOffsets, inAllocator);
                break;
            default:
                throw new IllegalArgumentException(batch.direction + " unsupported in loading");
        }
        progress.relationshipBatchImported(batch.length >> 1);
    }

    private void initOut() {
        if ((initStatus & INIT_OUT) == 0) {
            initStatus |= INIT_OUT;
//            outDegrees = new int[maxId];
//            idMap.readOutDegrees(idBase, outDegrees, maxId);
//            tracker.remove(idMap.releaseOutDegrees());
            outTargets = new CompressedLongArray[numberOfElements];
            outAllocator.prepare();
        }
    }

    private void initIn() {
        if ((initStatus & INIT_IN) == 0) {
            initStatus |= INIT_IN;
//            inDegrees = new int[maxId];
//            idMap.readInDegrees(idBase, inDegrees, maxId);
//            tracker.remove(idMap.releaseInDegrees());
            inTargets = new CompressedLongArray[numberOfElements];
            inAllocator.prepare();
        }
    }

    private void addRelationship(
            RelationshipsBatch batch, CompressedLongArray[] targets, int[] degrees,
            HugeLongArray offsets, ByteArray.LocalAllocator allocator) {
        final long[] sourceAndTargets = batch.sourceAndTargets;
        final int length = batch.length;
        for (int i = 0; i < length; i += 2) {
            final long source = sourceAndTargets[i];
            final long target = sourceAndTargets[i + 1];
            addRelationship(source, target, targets, degrees, offsets, allocator);
        }
    }

    private void addRelationship(
            long source, long target, CompressedLongArray[] targets, int[] degrees,
            HugeLongArray offsets, ByteArray.LocalAllocator allocator) {
        int localFrom = (int) (source - startId);
        assert localFrom < numberOfElements;
        final long targetId = idMap.toHugeMappedNodeId(target);
        if (targetId != -1L) {
            int degree = addTarget(localFrom, targetId, targets, degrees);
            if (degree >= degrees[localFrom]) {
                flushBuffer = applyVariableDeltaEncoding(flushBuffer, targets[localFrom], allocator, source, offsets);
                targets[localFrom].release();
                targets[localFrom] = null;
            }
            ++size;
        }
    }

    private int addTarget(int source, long targetId, CompressedLongArray[] targets, int[] degrees) {
        CompressedLongArray target = targets[source];
        if (target == null) {
            targets[source] = new CompressedLongArray(targetId, degrees[source]);
            return 1;
        }
        return target.add(targetId);
    }

    private void flush(
            CompressedLongArray[] targetIds,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator) {
        if (targetIds != null && offsets != null && allocator != null) {
            long[] buffer = flushBuffer;
            for (int i = 0, length = targetIds.length; i < length; i++) {
                CompressedLongArray targets = targetIds[i];
                if (targets != null) {
                    buffer = applyVariableDeltaEncoding(buffer, targets, allocator, i + startId, offsets);
                    targetIds[i] = null;
                }
            }
        }
    }

    private long[] applyVariableDeltaEncoding(
            long[] buffer,
            CompressedLongArray array,
            ByteArray.LocalAllocator allocator,
            long sourceId,
            HugeLongArray offsets) {
        buffer = array.ensureBufferSize(buffer);
        int bufLen = array.uncompress(buffer);
        array.release();
        if (bufLen > 0) {
            long adjacencyIdx = applyDeltaEncoding(buffer, bufLen, allocator);
            offsets.set(sourceId, adjacencyIdx);
        }
        return buffer;
    }

    private long applyDeltaEncoding(
            long[] targets,
            int length,
            ByteArray.LocalAllocator allocator) {
        Arrays.sort(targets, 0, length);

        long delta = targets[0];
        int writePos = 1;
        long requiredBytes = 4L + vSize(delta);  // length as full-int

        for (int i = 1; i < length; ++i) {
            long nextDelta = targets[i];
            long value = targets[writePos] = nextDelta - delta;
            if (value > 0L) {
                ++writePos;
                requiredBytes += vSize(value);
                delta = nextDelta;
            }
        }

        int degree = writePos;

        long adjacencyIdx = allocator.allocate(requiredBytes);
        ByteArray.BulkAdder bulkAdder = allocator.adder;
        bulkAdder.addUnsignedInt(degree);
        bulkAdder.addVLongs(targets, degree);
        return adjacencyIdx;
    }

    @Override
    public String toString() {
        return "PerThreadBuilder{" +
                "idBase=" + startId +
                ", maxId=" + (startId + (long) numberOfElements) +
                ", size=" + size +
                '}';
    }

    //    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder().append('[');
//
//        boolean first = true;
//        for (int i = 0; i < targetIds.length; i++) {
//            GrowableLongArray targetsRef = targetIds[i];
//            if (targetsRef != null && targetsRef.length() > 0) {
//                if (!first) {
//                    sb.append(',');
//                } else {
//                    first = false;
//                }
//                final long[] targets = Arrays.copyOf(targetsRef.longs(), targetsRef.length());
//                sb.append(i + idBase).append(":=").append(Arrays.toString(targets));
//            }
//        }
//
//        return sb.append(']').toString();
//    }
}


final class LoadDegreeUndirected implements PerThreadRelationshipBuilder.DegreeLoader {

    static PerThreadRelationshipBuilder.DegreeLoader INSTANCE = new LoadDegreeUndirected();

    private LoadDegreeUndirected() {
    }

    @Override
    public void load(NodeCursor cursor, LoadRelationships loader, int localId, int[] outDegrees, int[] inDegrees) {
        outDegrees[localId] = loader.degreeBoth(cursor);
    }
}

final class LoadDegreeBoth implements PerThreadRelationshipBuilder.DegreeLoader {

    static PerThreadRelationshipBuilder.DegreeLoader INSTANCE = new LoadDegreeBoth();

    private LoadDegreeBoth() {
    }

    @Override
    public void load(
            final NodeCursor cursor,
            final LoadRelationships loader,
            int localId,
            int[] outDegrees,
            int[] inDegrees) {
        outDegrees[localId] = loader.degreeOut(cursor);
        inDegrees[localId] = loader.degreeIn(cursor);
    }
}

final class LoadDegreeOut implements PerThreadRelationshipBuilder.DegreeLoader {

    static PerThreadRelationshipBuilder.DegreeLoader INSTANCE = new LoadDegreeOut();

    private LoadDegreeOut() {
    }

    @Override
    public void load(
            final NodeCursor cursor,
            final LoadRelationships loader,
            int localId,
            int[] outDegrees,
            int[] inDegrees) {
        outDegrees[localId] = loader.degreeOut(cursor);
    }
}

final class LoadDegreeIn implements PerThreadRelationshipBuilder.DegreeLoader {

    static PerThreadRelationshipBuilder.DegreeLoader INSTANCE = new LoadDegreeIn();

    private LoadDegreeIn() {
    }

    @Override
    public void load(
            final NodeCursor cursor,
            final LoadRelationships loader,
            int localId,
            int[] outDegrees,
            int[] inDegrees) {
        inDegrees[localId] = loader.degreeIn(cursor);
    }
}

final class LoadDegreeNone implements PerThreadRelationshipBuilder.DegreeLoader {

    static PerThreadRelationshipBuilder.DegreeLoader INSTANCE = new LoadDegreeNone();

    private LoadDegreeNone() {
    }

    @Override
    public void load(
            final NodeCursor cursor,
            final LoadRelationships loader,
            int localId,
            int[] outDegrees,
            int[] inDegrees) {
    }
}
