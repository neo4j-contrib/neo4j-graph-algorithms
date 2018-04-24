package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.AbstractCollection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public final class RelationshipsScanner extends StatementAction {

    private final ArrayBlockingQueue<RelationshipsBatch> pool;
    private final ArrayBlockingQueue<RelationshipsBatch>[] threadQueues;
    private final int threadsShift;
    private final int batchSize;

    private final HugeIdMap idMap;
    private final boolean loadOut;
    private final boolean loadIn;

    private final LongsBuffer out;
    private final LongsBuffer in;
    private final Direction outDirection;
    private final Direction inDirection;
    private final GraphSetup setup;

    RelationshipsScanner(
            GraphDatabaseAPI api,
            GraphSetup setup,
            HugeIdMap idMap,
            int maxInFlight,
            ArrayBlockingQueue<RelationshipsBatch>[] threadQueues,
            int perThreadSize) {
        super(api);
        assert BitUtil.isPowerOfTwo(threadQueues.length);
        assert BitUtil.isPowerOfTwo(perThreadSize);
        this.setup = setup;
        this.idMap = idMap;
        this.threadQueues = threadQueues;
        this.batchSize = 1 << 14;
        this.threadsShift = Integer.numberOfTrailingZeros(perThreadSize);
        this.pool = newPool(maxInFlight);
        if (setup.loadAsUndirected) {
            this.out = new LongsBuffer(threadQueues.length, batchSize);
            this.in = out;
            this.outDirection = Direction.OUTGOING;
            this.inDirection = Direction.OUTGOING;
            this.loadOut = true;
            this.loadIn = true;
        } else {
            this.outDirection = Direction.OUTGOING;
            this.inDirection = Direction.INCOMING;
            LongsBuffer out = null;
            LongsBuffer in = null;
            boolean loadOut = false;
            boolean loadIn = false;
            if (setup.loadOutgoing) {
                out = new LongsBuffer(threadQueues.length, batchSize);
                loadOut = true;
            }
            if (setup.loadIncoming) {
                in = new LongsBuffer(threadQueues.length, batchSize);
                loadIn = true;
            }
            this.out = out != null ? out : LongsBuffer.EMPTY;
            this.in = in != null ? in : LongsBuffer.EMPTY;
            this.loadOut = loadOut;
            this.loadIn = loadIn;
        }
    }

    @Override
    public String threadName() {
        return "relationship-store-scan";
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        boolean wasInterrupted = false;
        try {
            scanRelationships(transaction);
            sendLastBatch();
        } catch (KernelException e) {
            ExceptionUtil.throwKernelException(e);
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

    private void scanRelationships(final KernelTransaction transaction) throws
            PropertyKeyIdNotFoundKernelException,
            InterruptedException {
        TokenRead tokenRead = transaction.tokenRead();

        int typeId = setup.loadAnyRelationshipType()
                ? Read.ANY_RELATIONSHIP_TYPE
                : tokenRead.relationshipType(setup.relationshipType);


        CursorFactory cursors = transaction.cursors();
        try (RelationshipScanCursor rc = cursors.allocateRelationshipScanCursor();
             PropertyCursor pc = cursors.allocatePropertyCursor()) {
            transaction.dataRead().relationshipTypeScan(typeId, rc);
            while (rc.next()) {
                long source = rc.sourceNodeReference();
                long target = rc.targetNodeReference();
                if (loadOut) {
                    long graphSource = idMap.toHugeMappedNodeId(source);
                    if (graphSource != -1L) {
                        batchRelationship(graphSource, target, out, outDirection);
                    }
                }
                if (loadIn) {
                    long graphTarget = idMap.toHugeMappedNodeId(target);
                    if (graphTarget != -1L) {
                        batchRelationship(graphTarget, source, in, inDirection);
                    }
                }
//                rc.properties(pc);
//                while (pc.next()) {
//                    final String propName = tokenRead.propertyKeyName(pc.propertyKey());
//                    final Value value = pc.propertyValue();
//                    System.out.printf("prop (%d)->(%d): [%s] = %s%n", source, target, propName, value);
//                }
            }

        }
    }

    private void batchRelationship(long source, long target, LongsBuffer buffer, Direction direction)
    throws InterruptedException {
        int threadIndex = (int) (source >>> threadsShift);
        int len = buffer.addRelationship(threadIndex, source, target);
        if (len >= batchSize) {
            sendRelationship(threadIndex, len, buffer, direction);
        }
    }

    private void sendLastBatch() throws InterruptedException {
        out.drainAndRelease((index, target, length) -> sendRelationship(index, target, length, Direction.OUTGOING));
        if (in != out) {
            in.drainAndRelease((index, target, length) -> sendRelationship(index, target, length, Direction.INCOMING));
        }
    }

    private void sendSentinels() throws InterruptedException {
        for (int i = 0; i < threadQueues.length; i++) {
//            System.out.println("sending sentinel to thread #" + i);
            spinWaitSend(threadQueues[i], RelationshipsBatch.SENTINEL);
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
}
