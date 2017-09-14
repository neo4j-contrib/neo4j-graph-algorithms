package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.core.huge.NodeLoader.NodeBatch;

import java.util.Queue;
import java.util.concurrent.locks.LockSupport;

final class NodeImporter implements Runnable {

    private Queue<NodeBatch> queue;

    private long[][] algoToNeo;
    private LongLongMap[] neoToAlgo;
    private final long nodeStart;
    private long imported;

    NodeImporter(
            long nodeStart,
            Queue<NodeBatch> queue,
            long[][] algoToNeo,
            LongLongMap[] neoToAlgo) {
        this.nodeStart = nodeStart;
        this.queue = queue;
        this.neoToAlgo = neoToAlgo;
        this.algoToNeo = algoToNeo;
    }

    @Override
    public void run() {
        while (true) {
            NodeBatch batch = queue.poll();
            while (batch == null) {
                // wait 1ms before repolling
                LockSupport.parkNanos(1_000_000L);
                batch = queue.poll();
            }
            if (batch.index == -1) {
                return;
            }
            accept(batch);
        }
    }

    private void accept(NodeBatch batch) {
        long[] algoToNeoBatch = batch.batch;
        LongLongHashMap neoToAlgoBatch = new LongLongHashMap(algoToNeoBatch.length);
        long nodeStart = this.nodeStart;
        long imported = this.imported;
        for (long neoId : algoToNeoBatch) {
            long algoId = nodeStart + imported++;
            neoToAlgoBatch.put(neoId, algoId);
        }
        this.imported = imported;
        algoToNeo[batch.index] = algoToNeoBatch;
        neoToAlgo[batch.index] = neoToAlgoBatch;
    }
}
