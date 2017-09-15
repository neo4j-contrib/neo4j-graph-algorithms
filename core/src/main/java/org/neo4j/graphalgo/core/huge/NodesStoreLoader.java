package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


final class NodesStoreLoader implements Runnable {

    private static final NodeBatch SENTINEL = new NodeBatch(null, -1, 0);

    private final GraphDatabaseAPI api;
    private final ThreadToStatementContextBridge contextBridge;
    private final int nodeLabelId;
    private final int batchSize;

    private final BlockingQueue<NodeBatch> queue;
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
        capacity = concurrency;
        queue = new ArrayBlockingQueue<>(BitUtil.nextHighestPowerOfTwo(concurrency), true);
    }

    @Override
    public void run() {
        try (final Transaction tx = api.beginTx();
             Statement statement = contextBridge.get()) {
            run(statement.readOperations());
            tx.success();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run(ReadOperations readOp) throws InterruptedException {
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
            NodeBatch nodeBatch = new NodeBatch(batch, index, i);
            queue.put(nodeBatch);
        }

        int cap = capacity;
        // flood the queue with sentinels
        while (cap --> 0) {
            queue.put(SENTINEL);
        }
    }

    BlockingQueue<NodeBatch> queue() {
        return queue;
    }

    static final class NodeBatch {
        final long[] batch;
        final int index;
        final int length;

        private NodeBatch(long[] batch, int index, int length) {
            this.batch = batch;
            this.index = index;
            this.length = length;
        }
    }
}
