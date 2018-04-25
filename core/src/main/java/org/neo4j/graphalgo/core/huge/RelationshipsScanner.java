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
import org.neo4j.graphalgo.core.utils.StatementAction;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public final class RelationshipsScanner extends StatementAction {

    private final ArrayBlockingQueue<RelationshipsBatch> pool;
    private final ArrayBlockingQueue<RelationshipsBatch>[] threadQueues;
    private int[][] outDegrees;
    private int[][] inDegrees;
    private final long[] baseIds;
    private final int threadsShift;
    private final int batchSize;

    private final HugeIdMap idMap;

    private final DegreeLoader loader;
    private final Emit emit;
    private final GraphSetup setup;

    RelationshipsScanner(
            GraphDatabaseAPI api,
            GraphSetup setup,
            HugeIdMap idMap,
            int maxInFlight,
            int batchSize,
            ArrayBlockingQueue<RelationshipsBatch>[] threadQueues,
            int[][] outDegrees,
            int[][] inDegrees,
            int perThreadSize) {
        super(api);
        assert (batchSize & 1) == 0 : "batchSize must be even";
        assert BitUtil.isPowerOfTwo(perThreadSize);
        this.setup = setup;
        this.idMap = idMap;
        this.threadQueues = threadQueues;
        this.outDegrees = outDegrees;
        this.inDegrees = inDegrees;
        this.baseIds = new long[threadQueues.length];
        Arrays.setAll(baseIds, i -> (long) i * perThreadSize);
        this.batchSize = batchSize;
        this.threadsShift = Integer.numberOfTrailingZeros(perThreadSize);
        this.pool = newPool(maxInFlight);
        Map.Entry<DegreeLoader, Emit> entry = setupDegreeLoaderAndEmit(setup, batchSize, threadQueues.length);
        this.loader = entry.getKey();
        this.emit = entry.getValue();
    }

    private Map.Entry<DegreeLoader, Emit> setupDegreeLoaderAndEmit(GraphSetup setup, int batchSize, int threadSize) {
        final DegreeLoader loader;
        final Emit emit;
        if (setup.loadAsUndirected) {
            this.inDegrees = this.outDegrees;
            loader = new LoadDegreeBoth();
            LongsBuffer buffer = new LongsBuffer(threadSize, batchSize);
            emit = new EmitBoth(buffer, Direction.OUTGOING, buffer, Direction.OUTGOING);
        } else {
            if (setup.loadOutgoing) {
                if (setup.loadIncoming) {
                    loader = new LoadDegreeBoth();
                    emit = new EmitBoth(
                            new LongsBuffer(threadSize, batchSize),
                            Direction.OUTGOING,
                            new LongsBuffer(threadSize, batchSize),
                            Direction.INCOMING);

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
    public String threadName() {
        return "relationship-store-scan";
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        boolean wasInterrupted = false;
        try {
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

    private void loadDegrees(final KernelTransaction transaction) {
        forAllRelationships(transaction, this::loadDegree);
        // remove references so that GC can eventually reclaim memory
        inDegrees = null;
        outDegrees = null;
    }

    private void loadDegree(RelationshipScanCursor rc) {
        while (rc.next()) {
            long source = idMap.toHugeMappedNodeId(rc.sourceNodeReference());
            long target = idMap.toHugeMappedNodeId(rc.targetNodeReference());
            if (source != -1L && target != -1L) {
                loader.load(source, target, this);
            }
        }
    }

    private void scanRelationships(final KernelTransaction transaction) throws InterruptedException {
        forAllRelationships(transaction, this::scanRelationship);
    }

    private void scanRelationship(RelationshipScanCursor rc) throws InterruptedException {
        while (rc.next()) {
            long source = idMap.toHugeMappedNodeId(rc.sourceNodeReference());
            long target = idMap.toHugeMappedNodeId(rc.targetNodeReference());
            if (source != -1L && target != -1L) {
                emit.emit(source, target, this);
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

    private int threadIndex(long nodeId) {
        return (int) (nodeId >>> threadsShift);
    }

    private void incOutDegree(long nodeId) {
        int threadIdx = threadIndex(nodeId);
        ++outDegrees[threadIdx][(int) (nodeId - baseIds[threadIdx])];
    }

    private void incInDegree(long nodeId) {
        int threadIdx = threadIndex(nodeId);
        ++inDegrees[threadIdx][(int) (nodeId - baseIds[threadIdx])];
    }

    private void batchRelationship(long source, long target, LongsBuffer buffer, Direction direction)
    throws InterruptedException {
        int threadIndex = threadIndex(source);
        int len = buffer.addRelationship(threadIndex, source, target);
        if (len >= batchSize) {
            sendRelationship(threadIndex, len, buffer, direction);
        }
    }

    private void sendLastBatch() throws InterruptedException {
        emit.emitLastBatch(this);
    }

    private void sendSentinels() throws InterruptedException {
        for (ArrayBlockingQueue<RelationshipsBatch> threadQueue : threadQueues) {
            spinWaitSend(threadQueue, RelationshipsBatch.SENTINEL);
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
                    relationshipsBatch.sourceAndTargets = null;
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
//        System.out.println("sending batch of length " + length + " to thread #" + threadIndex);
        RelationshipsBatch batch = nextRelationshipBatch();
        long[] newBuffer = setRelationshipBatch(batch, buffer.get(threadIndex), length, direction);
        buffer.reset(threadIndex, newBuffer);
        spinWaitSend(threadQueues[threadIndex], batch);
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
//        System.out.println("sending batch of length " + length + " to thread #" + threadIndex);
        RelationshipsBatch batch = nextRelationshipBatch();
        setRelationshipBatch(batch, targets, length, direction);
        spinWaitSend(threadQueues[threadIndex], batch);
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
        if (loaded.sourceAndTargets == null) {
            loaded.sourceAndTargets = batch;
            return new long[length];
        } else {
            long[] sourceAndTargets = loaded.sourceAndTargets;
            loaded.sourceAndTargets = batch;
            return sourceAndTargets;
        }
    }

    private void spinWaitSend(ArrayBlockingQueue<RelationshipsBatch> queue, RelationshipsBatch rel)
    throws InterruptedException {
        queue.put(rel);
//        while (true) {
//            if (queue.offer(rel)) {
//                return;
//            }
//        }
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
        void emit(long source, long target, RelationshipsScanner scanner) throws InterruptedException;

        void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException;
    }

    private static final class EmitBoth implements Emit {
        private final LongsBuffer outBuffer;
        private final Direction outDirection;
        private final LongsBuffer inBuffer;
        private final Direction inDirection;

        private EmitBoth(LongsBuffer outBuffer, Direction outDirection, LongsBuffer inBuffer, Direction inDirection) {
            this.outBuffer = outBuffer;
            this.outDirection = outDirection;
            this.inBuffer = inBuffer;
            this.inDirection = inDirection;
        }

        @Override
        public void emit(long source, long target, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(source, target, outBuffer, outDirection);
            scanner.batchRelationship(target, source, inBuffer, inDirection);
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
        public void emit(long source, long target, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(source, target, buffer, Direction.OUTGOING);
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
        public void emit(long source, long target, RelationshipsScanner scanner)
        throws InterruptedException {
            scanner.batchRelationship(target, source, buffer, Direction.INCOMING);
        }
        @Override
        public void emitLastBatch(RelationshipsScanner scanner) throws InterruptedException {
            buffer.drainAndRelease(scanner::sendRelationshipOut);
        }
    }

    private static final class EmitNone implements Emit {
        private EmitNone() {
        }

        @Override
        public void emit(long source, long target, RelationshipsScanner scanner) {
        }

        @Override
        public void emitLastBatch(RelationshipsScanner scanner) {
        }
    }
}
