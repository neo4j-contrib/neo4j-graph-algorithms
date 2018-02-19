package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RenamingRunnable;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import static org.neo4j.graphalgo.core.utils.paged.DeltaEncoding.vSize;

final class PerThreadRelationshipBuilder implements RenamingRunnable<Void> {

    private final int threadIndex;
    private final long idBase;
    private final int maxId;
    private final ArrayBlockingQueue<RelationshipsBatch> queue;
    private final HugeIdMap idMap;
    private final ImportProgress progress;

    private CompressedLongArray[] outTargets;
    private final HugeLongArray outOffsets;
    private final ByteArray.LocalAllocator outAllocator;
    private CompressedLongArray[] inTargets;
    private final HugeLongArray inOffsets;
    private final ByteArray.LocalAllocator inAllocator;

    private long size;

    PerThreadRelationshipBuilder(
            int threadIndex,
            long idBase,
            int maxId,
            ArrayBlockingQueue<RelationshipsBatch> queue,
            ImportProgress progress,
            HugeIdMap idMap,
            HugeLongArray outOffsets,
            ByteArray outAdjacency,
            HugeLongArray inOffsets,
            ByteArray inAdjacency) {
        this.threadIndex = threadIndex;
        this.idBase = idBase;
        this.maxId = maxId;
        this.queue = queue;
        this.idMap = idMap;
        this.progress = progress;
        this.outOffsets = outOffsets;
        this.inOffsets = inOffsets;
        this.outAllocator = outAdjacency != null ? outAdjacency.newAllocator() : null;
        this.inAllocator = inAdjacency != null ? inAdjacency.newAllocator() : null;
    }

    @Override
    public Void doRun() {
        try {
            while (true) {
                try (RelationshipsBatch relationship = pollNext()) {
                    if (relationship == RelationshipsBatch.SENTINEL) {
                        break;
                    }
                    addRelationship(relationship);
                }
            }
            flush(outTargets, outOffsets, outAllocator);
            flush(inTargets, inOffsets, inAllocator);
            outTargets = null;
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
        return null;
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
                addRelationship(batch, outTargets);
                break;
            case INCOMING:
                initIn();
                addRelationship(batch, inTargets);
                break;
            default:
                throw new IllegalArgumentException(batch.direction + " unsupported in loading");
        }
        progress.relationshipBatchImported(batch.length >> 1);
    }

    private void initOut() {
        if (outTargets == null && outAllocator != null) {
            outTargets = new CompressedLongArray[maxId];
            outAllocator.prepare();
        }
    }

    private void initIn() {
        if (inTargets == null && inAllocator != null) {
            inTargets = new CompressedLongArray[maxId];
            inAllocator.prepare();
        }
    }

    private void addRelationship(RelationshipsBatch batch, CompressedLongArray[] targets) {
        final long[] sourceAndTargets = batch.sourceAndTargets;
        final int length = batch.length;
        for (int i = 0; i < length; i += 2) {
            final long source = sourceAndTargets[i];
            final long target = sourceAndTargets[i + 1];
            addRelationship(source, target, targets);
        }
    }

    private void addRelationship(long source, long target, CompressedLongArray[] targets) {
        int localFrom = (int) (source - idBase);
        assert localFrom < maxId;
        final long targetId = idMap.toHugeMappedNodeId(target);
        if (targetId != -1L) {
            addTarget(localFrom, targetId, targets);
            ++size;
        }
    }

    private void addTarget(int source, long targetId, CompressedLongArray[] targets) {
        CompressedLongArray target = targets[source];
        if (target == null) {
            targets[source] = new CompressedLongArray(targetId);
        } else {
            target.add(targetId);
        }
    }

    private void flush(
            CompressedLongArray[] targetIds,
            HugeLongArray offsets,
            ByteArray.LocalAllocator allocator) {
        if (targetIds != null && offsets != null && allocator != null) {
            long[] buffer = new long[0];
            for (int i = 0, length = targetIds.length; i < length; i++) {
                CompressedLongArray targets = targetIds[i];
                if (targets != null) {
                    buffer = applyVariableDeltaEncoding(buffer, targets, allocator, i + idBase, offsets);
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
                "idBase=" + idBase +
                ", maxId=" + (idBase + (long) maxId) +
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
