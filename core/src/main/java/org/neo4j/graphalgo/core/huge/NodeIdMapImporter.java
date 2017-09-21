package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.graphalgo.core.huge.NodesStoreLoader.NodeBatch;
import org.neo4j.graphalgo.core.utils.container.TrackingLongLongHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.PageUtil.pageIndex;

final class NodeIdMapImporter implements Runnable {

    private final BlockingQueue<NodeBatch> queue;
    private final BlockingQueue<NodeMapBatch> mapQueue;
    private final BlockingQueue<NodeMapBatch>[] mapQueues;
    private final AtomicInteger livingSiblings;
    private final long[][] algoToNeo;
    private final LongLongMap[] neoToAlgo;
    private final AllocationTracker tracker;
    private final ImportProgress progress;

    private final int pageShift;
    private final int mapMask;
    private final int localIndex;

    static Collection<Runnable> createImporterTasks(
            int mapQueuesLen,
            BlockingQueue<NodeBatch> queue,
            long[][] algoToNeo,
            final LongLongMap[] neoToAlgo,
            AllocationTracker tracker,
            ImportProgress progress) {
        AtomicInteger livingSiblings = new AtomicInteger(0);
        //noinspection unchecked
        BlockingQueue<NodeMapBatch>[] queues = new BlockingQueue[mapQueuesLen];
        Collection<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < mapQueuesLen; i++) {
            BlockingQueue<NodeMapBatch> mapQueue = new ArrayBlockingQueue<>(mapQueuesLen << 4);
            queues[i] = mapQueue;
            NodeIdMapImporter mapImporter = new NodeIdMapImporter(
                    HugeIdMap.PAGE_SIZE,
                    i,
                    queue,
                    queues,
                    livingSiblings,
                    algoToNeo,
                    neoToAlgo,
                    tracker,
                    progress
            );
            tasks.add(mapImporter);
        }
        return tasks;
    }

    private NodeIdMapImporter(
            int pageSize,
            int localIndex,
            BlockingQueue<NodeBatch> queue,
            BlockingQueue<NodeMapBatch>[] mapQueues,
            AtomicInteger livingSiblings,
            long[][] algoToNeo,
            final LongLongMap[] neoToAlgo,
            AllocationTracker tracker,
            ImportProgress progress) {
        assert BitUtil.isPowerOfTwo(mapQueues.length);
        this.queue = queue;
        this.mapQueues = mapQueues;
        this.mapQueue = mapQueues[localIndex];
        this.livingSiblings = livingSiblings;
        this.algoToNeo = algoToNeo;
        this.neoToAlgo = neoToAlgo;
        this.tracker = tracker;
        this.progress = progress;
        this.pageShift = Integer.numberOfTrailingZeros(pageSize);
        this.mapMask = mapQueues.length - 1;
        this.localIndex = localIndex;
    }

    @Override
    public void run() {
        livingSiblings.incrementAndGet();
        boolean markedDead = false;
        try {
            while (true) {
                drainMapQueue(mapQueue, mapMask);
                NodeBatch batch = queue.poll();
                if (batch != null) {
                    if (batch.index == -1) {
                        livingSiblings.decrementAndGet();
                        markedDead = true;
                        drain(livingSiblings);
                        return;
                    }
                    accept(batch);
                }
            }
        } catch (InterruptedException e) {
            if (!markedDead) {
                livingSiblings.decrementAndGet();
            }
            Thread.currentThread().interrupt();
        }
    }


    // We won't receive upstream batches but other threads could still have
    // partial batches for us. We need to work until all siblings are dead.
    private void drain(AtomicInteger livingSiblings) throws InterruptedException {
        NodeMapBatch mapBatch;
        while (livingSiblings.get() != 0) {
            mapBatch = mapQueue.poll(1, TimeUnit.MILLISECONDS);
            if (mapBatch != null) {
                accept(mapBatch);
            }
        }
        drainMapQueue(mapQueue, mapMask);
    }

    private void drainMapQueue(BlockingQueue<NodeMapBatch> batches, int mapMask)
    throws InterruptedException {
        NodeMapBatch mapBatch;
        while ((mapBatch = batches.poll()) != null) {
            sendMapBatch(mapBatch, mapBatch.index, mapMask);
        }
    }

    private void accept(NodeBatch batch) throws InterruptedException {
        trackBatch(batch);

        int pageShift = this.pageShift;
        int mapMask = this.mapMask;

        long[] algoToNeoBatch = batch.batch;
        int length = batch.length;

        int mapOffset = 0;
        int mapPage = pageIndex(algoToNeoBatch[0], pageShift);
        final long nodeId = batch.index << pageShift;
        long mapAlgoId = nodeId;

        for (int i = 1; i < length; i++) {
            long neoId = algoToNeoBatch[i];
            int nextMapPage = pageIndex(neoId, pageShift);
            if (nextMapPage != mapPage) {
                NodeMapBatch mapBatch = new NodeMapBatch(
                        mapPage,
                        algoToNeoBatch,
                        mapOffset,
                        i,
                        mapAlgoId);
                sendMapBatch(mapBatch, mapPage, mapMask);
                mapOffset = i;
                mapPage = nextMapPage;
                mapAlgoId = nodeId + i;
            }
        }

        // always has at least one element
        NodeMapBatch lastBatch = new NodeMapBatch(
                mapPage,
                algoToNeoBatch,
                mapOffset,
                length,
                mapAlgoId
        );
        sendMapBatch(lastBatch, mapPage, mapMask);
        progress.nodeProgress(length);
        algoToNeo[batch.index] = batch.batch;
    }

    private void sendMapBatch(NodeMapBatch mapBatch, int mapPage, int mapMask)
    throws InterruptedException {
        int mapQueueIndex = mapPage & mapMask;
        if (mapQueueIndex == localIndex) {
            accept(mapBatch);
        } else {
            mapQueues[mapQueueIndex].put(mapBatch);
        }
    }

    private void accept(NodeMapBatch batch) {
        mapIds(
                batch.index,
                batch.nodes,
                batch.fromIndex,
                batch.toIndex,
                batch.firstAlgoId);
    }

    private void mapIds(
            int mapIndex,
            long[] algoToNeoBatch,
            int fromIndex,
            int toIndex,
            long nodeId) {
        LongLongMap neoToAlgo = this.neoToAlgo[mapIndex];
        if (neoToAlgo == null) {
            neoToAlgo = new TrackingLongLongHashMap(tracker);
            this.neoToAlgo[mapIndex] = neoToAlgo;
        }
        for (int i = fromIndex; i < toIndex; i++) {
            long neoId = algoToNeoBatch[i];
            long algoId = nodeId++;
            neoToAlgo.put(neoId, algoId);
        }
    }

    private void trackBatch(NodeBatch batch) {
        if (AllocationTracker.isTracking(tracker)) {
            // the array is always over-allocated to the full page size
            tracker.add(sizeOfLongArray(batch.batch.length));
        }
    }

    private static final class NodeMapBatch {
        private final int index;
        private final long[] nodes;
        private final int fromIndex;
        private final int toIndex;
        private final long firstAlgoId;

        private NodeMapBatch(
                int index,
                long[] nodes,
                int fromIndex,
                int toIndex,
                long firstAlgoId) {
            this.index = index;
            this.nodes = nodes;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.firstAlgoId = firstAlgoId;
        }
    }
}
