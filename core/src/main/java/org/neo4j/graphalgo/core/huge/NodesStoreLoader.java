package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


final class NodesStoreLoader implements Runnable, TerminationFlag {

    private static final NodeBatch SENTINEL = new NodeBatch(new long[0], -1);

    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;
    private final int nodeLabelId;
    private final int batchSize;

    // nodes are all read
    private final AtomicBoolean complete;
    private final Queue<NodeBatch> queue;
    private final int capacity;

    NodesStoreLoader(
            GraphDatabaseAPI api,
            int nodeLabelId,
            int concurrency,
            int batchSize) {
        this.api = api;
        this.contextBridge = api
                .getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class);
        this.nodeLabelId = nodeLabelId;
        this.batchSize = batchSize;
        complete = new AtomicBoolean(false);
        capacity = concurrency;
        queue = new ArrayBlockingQueue<>(BitUtil.nextHighestPowerOfTwo(concurrency));
    }

    @Override
    public void run() {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            run(statement.readOperations());
            tx.success();
        }
    }

    private void run(ReadOperations readOp) {
        complete.set(false);
        PrimitiveLongIterator nodeIds = nodeLabelId == ReadOperations.ANY_LABEL
                ? readOp.nodesGetAll()
                : readOp.nodesGetForLabel(nodeLabelId);

        int batchSize = this.batchSize;
        int batchIndex = 0;
        while (nodeIds.hasNext()) {
            long[] batch = new long[batchSize];
            int index = batchIndex++;
            int i = 0;
            while (i < batchSize && nodeIds.hasNext()) {
                batch[i++] = nodeIds.next();
            }
            //noinspection StatementWithEmptyBody -- spin-wait on free queue slot
            while (!queue.offer(new NodeBatch(batch, index)));
        }

        complete.set(true);

        int cap = capacity;
        //noinspection StatementWithEmptyBody - flood the queue with sentinels
        while (cap --> 0 && queue.offer(SENTINEL));
    }

    Queue<NodeBatch> queue() {
        return queue;
    }

    @Override
    public boolean running() {
        return !complete.get();
    }

    static final class NodeBatch {
        final long[] batch;
        final int index;

        private NodeBatch(final long[] batch, final int index) {
            this.batch = batch;
            this.index = index;
        }
    }
}
