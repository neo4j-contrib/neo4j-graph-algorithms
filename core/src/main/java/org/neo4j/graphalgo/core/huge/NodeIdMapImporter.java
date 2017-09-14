package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.core.huge.NodesStoreLoader.NodeBatch;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import java.util.Queue;
import java.util.concurrent.locks.LockSupport;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;

final class NodeIdMapImporter implements Runnable {

    private final Queue<NodeBatch> queue;
    private final AllocationTracker tracker;
    private final ImportProgress progress;
    private final long[][] algoToNeo;
    private final LongLongMap[] neoToAlgo;
    private final int pageShift;

    NodeIdMapImporter(
            int pageSize,
            Queue<NodeBatch> queue,
            long[][] algoToNeo,
            LongLongMap[] neoToAlgo,
            AllocationTracker tracker,
            ImportProgress progress) {
        this.queue = queue;
        this.neoToAlgo = neoToAlgo;
        this.algoToNeo = algoToNeo;
        this.tracker = tracker;
        this.progress = progress;
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
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
        trackBatch(batch);

        int length = batch.length;
        long[] algoToNeoBatch = batch.batch;
        LongLongHashMap neoToAlgoBatch = new LongLongHashMap(length);

        long nodeId = batch.index << pageShift;
        for (int i = 0; i < length; i++) {
            long neoId = algoToNeoBatch[i];
            long algoId = nodeId++;
            neoToAlgoBatch.put(neoId, algoId);
        }

        progress.nodeProgress(length);
        algoToNeo[batch.index] = algoToNeoBatch;
        neoToAlgo[batch.index] = neoToAlgoBatch;
    }

    private void trackBatch(NodeBatch batch) {
        if (AllocationTracker.isTracking(tracker)) {
            // the array is always over-allocated to the full page size
            tracker.add(sizeOfLongArray(batch.batch.length));
            tracker.add(HugeLongLongMap.estimateMemoryUsageOfPages(batch.length));
        }
    }
}
