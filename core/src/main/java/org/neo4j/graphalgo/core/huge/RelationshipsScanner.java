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

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.RenamesCurrentThread;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class RelationshipsScanner extends StatementAction {

    private final ArrayBlockingQueue<RelationshipsBatch> pool;
    private int[][] outDegrees;
    private int[][] inDegrees;
    private final int numberOfThreads;
    private final int batchSize;
    private final HugeIdMap idMap;
    private final DegreeLoader loader;
    private final Emit emit;
    private final GraphSetup setup;
    private final ImportProgress progress;
    private final AllocationTracker tracker;

    RelationshipsScanner(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            int maxInFlight,
            int batchSize,
            int numberOfThreads,
            int[][] outDegrees,
            int[][] inDegrees) {
        super(api);
        assert (batchSize % 3) == 0 : "batchSize must be divisible by three";
        this.setup = setup;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.outDegrees = outDegrees;
        this.inDegrees = inDegrees;
        this.numberOfThreads = numberOfThreads;
        this.batchSize = batchSize;
        this.pool = newPool(maxInFlight);
        Map.Entry<DegreeLoader, Emit> entry = setupDegreeLoaderAndEmit(setup, batchSize, numberOfThreads);
        this.loader = entry.getKey();
        this.emit = entry.getValue();
    }

    private Map.Entry<DegreeLoader, Emit> setupDegreeLoaderAndEmit(GraphSetup setup, int batchSize, int threadSize) {
        final DegreeLoader loader;
        final Emit emit;
        if (setup.loadAsUndirected) {
            loader = new LoadDegreeUndirected();
            emit = new EmitUndirected(threadSize, batchSize);
        } else {
            if (setup.loadOutgoing) {
                if (setup.loadIncoming) {
                    loader = new LoadDegreeBoth();
                    emit = new EmitBoth(threadSize, batchSize);
                } else {
                    loader = new LoadDegreeOut();
                    emit = new EmitOut(threadSize, batchSize);
                }
            } else {
                if (setup.loadIncoming) {
                    loader = new LoadDegreeIn();
                    emit = new EmitIn(threadSize, batchSize);
                } else {
                    loader = new LoadDegreeNone();
                    emit = new EmitNone();
                }
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(loader, emit);
    }

    @Override
    public final String threadName() {
        return "relationship-store-scan";
    }

    @Override
    public final void accept(final KernelTransaction transaction) {
        boolean wasInterrupted = false;
        try {
            prepareTransaction(transaction);
            loadDegrees(transaction);
            scanRelationships(transaction);
            sendLastBatch();
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }

        try {
            sendSentinels();
        } catch (InterruptedException e) {
            wasInterrupted = true;
        }

        releaseBufferPool();
        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    abstract void prepareTransaction(KernelTransaction transaction);

    abstract int threadIndex(long nodeId);

    abstract int threadLocalId(long nodeId);

    abstract void sendBatchToImportThread(int threadIndex, RelationshipsBatch rel) throws InterruptedException;

    private void loadDegrees(final KernelTransaction transaction) {
        try (Revert ignore = RenamesCurrentThread.renameThread("huge-scan-degrees")) {
            forAllRelationships(transaction, this::loadDegree);
        }
        // remove references so that GC can eventually reclaim memory
        if (inDegrees != null) {
            tracker.remove(sizeOfObjectArray(inDegrees.length));
            inDegrees = null;
        }
        if (outDegrees != null) {
            tracker.remove(sizeOfObjectArray(outDegrees.length));
            outDegrees = null;
        }
    }

    private void loadDegree(RelationshipScanCursor rc) {
        int imported = 0;
        while (rc.next()) {
            long source = idMap.toHugeMappedNodeId(rc.sourceNodeReference());
            long target = idMap.toHugeMappedNodeId(rc.targetNodeReference());
            if (source != -1L && target != -1L) {
                loader.load(source, target, this);
            }
            if (++imported == 100_000) {
                progress.relationshipsImported(100_000);
                imported = 0;
            }
        }
        progress.relationshipsImported(imported);
    }

    private void scanRelationships(final KernelTransaction transaction) throws InterruptedException {
        forAllRelationships(transaction, this::scanRelationship);
    }

    private void scanRelationship(RelationshipScanCursor rc) throws InterruptedException {
        while (rc.next()) {
            long source = idMap.toHugeMappedNodeId(rc.sourceNodeReference());
            long target = idMap.toHugeMappedNodeId(rc.targetNodeReference());
            if (source != -1L && target != -1L) {
                emit.emit(source, target, rc.relationshipReference(), this);
            }
        }
    }

    private <E extends Exception> void forAllRelationships(
            KernelTransaction transaction,
            ThrowingConsumer<RelationshipScanCursor, E> block) throws E {
        TokenRead tokenRead = transaction.tokenRead();

        int typeId = setup.loadAnyRelationshipType()
                ? Read.ANY_RELATIONSHIP_TYPE
                : tokenRead.relationshipType(setup.relationshipType);

        CursorFactory cursors = transaction.cursors();
        try (RelationshipScanCursor rc = cursors.allocateRelationshipScanCursor()) {
            transaction.dataRead().relationshipTypeScan(typeId, rc);
            block.accept(rc);
        }
    }

    private void incOutDegree(long nodeId) {
        ++outDegrees[threadIndex(nodeId)][threadLocalId(nodeId)];
    }

    private void incInDegree(long nodeId) {
        ++inDegrees[threadIndex(nodeId)][threadLocalId(nodeId)];
    }

    private void batchRelationship(long source, long target, long relId, LongsBuffer buffer, Direction direction)
    throws InterruptedException {
        int threadIndex = threadIndex(source);
        int len = buffer.addRelationship(threadIndex, source, target, relId);
        if (len >= batchSize) {
            sendRelationship(threadIndex, len, buffer, direction);
        }
    }

    private void sendLastBatch() throws InterruptedException {
        emit.emitLastBatch(this);
    }

    private void sendSentinels() throws InterruptedException {
        for (int i = 0; i < numberOfThreads; i++) {
            sendBatchToImportThread(i, RelationshipsBatch.SENTINEL);
        }
    }

    private void releaseBufferPool() {
        if (pool != null) {
            AbstractCollection<RelationshipsBatch> consumer = new AbstractCollection<RelationshipsBatch>() {
                @Override
                public Iterator<RelationshipsBatch> iterator() {
                    return Collections.emptyIterator();
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public boolean add(final RelationshipsBatch relationshipsBatch) {
                    relationshipsBatch.sourceTargetIds = null;
                    relationshipsBatch.length = 0;
                    return false;
                }
            };
            pool.drainTo(consumer);
        }
    }

    private void sendRelationship(
            int threadIndex,
            int length,
            LongsBuffer buffer,
            Direction direction)
    throws InterruptedException {
        if (length == 0) {
            return;
        }
        RelationshipsBatch batch = nextRelationshipBatch();
        long[] newBuffer = setRelationshipBatch(batch, buffer.get(threadIndex), length, direction);
        buffer.reset(threadIndex, newBuffer);
        sendBatchToImportThread(threadIndex, batch);
    }

    private void sendRelationshipOut(int threadIndex, long[] targets, int length) throws InterruptedException {
        sendRelationship(threadIndex, targets, length, Direction.OUTGOING);
    }

    private void sendRelationshipIn(int threadIndex, long[] targets, int length) throws InterruptedException {
        sendRelationship(threadIndex, targets, length, Direction.INCOMING);
    }

    private void sendRelationship(
            int threadIndex,
            long[] targets,
            int length,
            Direction direction)
    throws InterruptedException {
        if (length == 0) {
            return;
        }
        RelationshipsBatch batch = nextRelationshipBatch();
        setRelationshipBatch(batch, targets, length, direction);
        sendBatchToImportThread(threadIndex, batch);
    }

    private RelationshipsBatch nextRelationshipBatch() {
        RelationshipsBatch loaded;
        do {
            loaded = pool.poll();
        } while (loaded == null);
        return loaded;
    }

    private long[] setRelationshipBatch(RelationshipsBatch loaded, long[] batch, int length, Direction direction) {
        loaded.length = length;
        loaded.direction = direction;
        if (loaded.sourceTargetIds == null) {
            loaded.sourceTargetIds = batch;
            return new long[length];
        } else {
            long[] sourceAndTargets = loaded.sourceTargetIds;
            loaded.sourceTargetIds = batch;
            return sourceAndTargets;
        }
    }

    private static ArrayBlockingQueue<RelationshipsBatch> newPool(int capacity) {
        final ArrayBlockingQueue<RelationshipsBatch> rels = new ArrayBlockingQueue<>(capacity);
        int i = capacity;
        while (i-- > 0) {
            rels.add(new RelationshipsBatch(rels));
        }
        return rels;
    }

    private interface DegreeLoader {
        void load(long source, long target, RelationshipsScanner scanner);
    }

    private static final class LoadDegreeUndirected implements DegreeLoader {

        @Override
        public void load(long source, long target, RelationshipsScanner scanner) {
            scanner.incOutDegree(source);
            scanner.incOutDegree(target);
        }
    }

    private static final class LoadDegreeBoth implements DegreeLoader {

        @Override
        public void load(long source, long target, RelationshipsScanner scanner) {
            scanner.incOutDegree(source);
            scanner.incInDegree(target);
        }
    }

    private static final class LoadDegreeOut implements DegreeLoader {

        @Override
        public void load(long source, long target, RelationshipsScanner scanner) {
            scanner.incOutDegree(source);
        }
    }

    private static final class LoadDegreeIn implements DegreeLoader {

        @Override
        public void load(long source, long target, RelationshipsScanner scanner) {
            scanner.incInDegree(target);
        }
    }

    private static final class LoadDegreeNone implements DegreeLoader {

        @Override
        public void load(long source, long target, RelationshipsScanner scanner) {
        }
    }

    private interface Emit {
        void emit(long source, long target, long relId, RelationshipsScanner scanner) throws InterruptedException;

        void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException;
    }

    private static final class EmitUndirected implements Emit {
        private final LongsBuffer outBuffer;

        private EmitUndirected(int numBuckets, int batchSize) {
            this.outBuffer = new LongsBuffer(numBuckets, batchSize);
        }

        @Override
        public void emit(long source, long target, long relId, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(source, target, relId, outBuffer, Direction.OUTGOING);
            scanner.batchRelationship(target, source, relId, outBuffer, Direction.OUTGOING);
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException {
            outBuffer.drainAndRelease(scanner::sendRelationshipOut);
        }
    }

    private static final class EmitBoth implements Emit {
        private final LongsBuffer outBuffer;
        private final LongsBuffer inBuffer;

        private EmitBoth(int numBuckets, int batchSize) {
            this.outBuffer = new LongsBuffer(numBuckets, batchSize);
            this.inBuffer = new LongsBuffer(numBuckets, batchSize);
        }

        @Override
        public void emit(long source, long target, long relId, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(source, target, relId, outBuffer, Direction.OUTGOING);
            scanner.batchRelationship(target, source, relId, inBuffer, Direction.INCOMING);
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException {
            outBuffer.drainAndRelease(scanner::sendRelationshipOut);
            inBuffer.drainAndRelease(scanner::sendRelationshipIn);
        }
    }

    private static final class EmitOut implements Emit {

        private final LongsBuffer buffer;

        private EmitOut(int numBuckets, int batchSize) {
            buffer = new LongsBuffer(numBuckets, batchSize);
        }

        @Override
        public void emit(long source, long target, long relId, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(source, target, relId, buffer, Direction.OUTGOING);
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException {
            buffer.drainAndRelease(scanner::sendRelationshipOut);
        }
    }

    private static final class EmitIn implements Emit {

        private final LongsBuffer buffer;

        private EmitIn(int numBuckets, int batchSize) {
            buffer = new LongsBuffer(numBuckets, batchSize);
        }

        @Override
        public void emit(long source, long target, long relId, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(target, source, relId, buffer, Direction.INCOMING);
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException {
            buffer.drainAndRelease(scanner::sendRelationshipIn);
        }
    }

    private static final class EmitNone implements Emit {
        private EmitNone() {
        }

        @Override
        public void emit(long source, long target, long relId, RelationshipsScanner scanner) {
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) {
        }
    }
}

final class QueueingScanner extends RelationshipsScanner {

    private final BlockingQueue<RelationshipsBatch>[] threadQueues;
    private final int threadsShift;
    private final long threadsMask;

    QueueingScanner(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            int maxInFlight,
            int batchSize,
            BlockingQueue<RelationshipsBatch>[] threadQueues,
            int[][] outDegrees,
            int[][] inDegrees,
            int perThreadSize) {
        super(api, setup, progress, tracker, idMap, maxInFlight, batchSize, threadQueues.length, outDegrees, inDegrees);
        assert BitUtil.isPowerOfTwo(perThreadSize);
        this.threadQueues = threadQueues;
        this.threadsShift = Integer.numberOfTrailingZeros(perThreadSize);
        this.threadsMask = (long) (perThreadSize - 1);
    }

    @Override
    int threadIndex(long nodeId) {
        return (int) (nodeId >>> threadsShift);
    }

    @Override
    int threadLocalId(long nodeId) {
        return (int) (nodeId & threadsMask);
    }

    @Override
    void sendBatchToImportThread(int threadIndex, RelationshipsBatch rel) throws InterruptedException {
        threadQueues[threadIndex].put(rel);
    }

    @Override
    void prepareTransaction(final KernelTransaction transaction) {
    }
}

final class NonQueueingScanner extends RelationshipsScanner {

    private final PerThreadRelationshipBuilder builder;

    NonQueueingScanner(
            GraphDatabaseAPI api,
            GraphSetup setup,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            int maxInFlight,
            int batchSize,
            PerThreadRelationshipBuilder builder,
            int[][] outDegrees,
            int[][] inDegrees) {
        super(api, setup, progress, tracker, idMap, maxInFlight, batchSize, 1, outDegrees, inDegrees);
        this.builder = builder;
    }

    @Override
    int threadIndex(long nodeId) {
        return 0;
    }

    @Override
    int threadLocalId(long nodeId) {
        return (int) nodeId;
    }

    @Override
    void sendBatchToImportThread(int threadIndex, RelationshipsBatch rel) {
        builder.pushBatch(rel);
    }

    @Override
    void prepareTransaction(final KernelTransaction transaction) {
        builder.useKernelTransaction(transaction);
    }
}
