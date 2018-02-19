package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.ByteArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

final class ScanningRelationshipImporter {

    private static final long MAX_BATCH_SIZE = 2_000_000_000L;
    private static final int PER_THREAD_IN_FLIGHT = 1 << 4;

    private final GraphDimensions dimensions;
    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final ImportProgress progress;
    private final HugeIdMap idMap;
    private final HugeLongArray outOffsets;
    private final ByteArray outAdjacency;
    private final HugeLongArray inOffsets;
    private final ByteArray inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    private ScanningRelationshipImporter(
            GraphDimensions dimensions,
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            HugeLongArray outOffsets,
            ByteArray outAdjacency,
            HugeLongArray inOffsets,
            ByteArray inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        this.dimensions = dimensions;
        this.setup = setup;
        this.api = api;
        this.progress = progress;
        this.idMap = idMap;
        this.outOffsets = outOffsets;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.inAdjacency = inAdjacency;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    static ScanningRelationshipImporter create(
            GraphDimensions dimensions,
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            HugeIdMap idMap,
            HugeLongArray outOffsets,
            ByteArray outAdjacency,
            HugeLongArray inOffsets,
            ByteArray inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            return null;
        }
        return new ScanningRelationshipImporter(
                dimensions, setup, api, progress, idMap,
                outOffsets, outAdjacency, inOffsets, inAdjacency,
                threadPool, concurrency);
    }

    void run() {
        long targetThreads = (long) concurrency;
        long batchSize = ParallelUtil.threadSize(targetThreads, dimensions.allNodesCount());
        batchSize = BitUtil.nextHighestPowerOfTwo(batchSize);
        while (batchSize > MAX_BATCH_SIZE) {
            targetThreads <<= 1L;
            batchSize >>= 1L;
        }
        if (targetThreads > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Can't create " + targetThreads + " threads");
        }

        run((int) targetThreads, (int) batchSize);
    }

    private void run(int threads, int batchSize) {
        int inFlight = threads * PER_THREAD_IN_FLIGHT;
        long idBase = 0L;
        //noinspection unchecked
        final ArrayBlockingQueue<RelationshipsBatch>[] queues = new ArrayBlockingQueue[threads];
        final PerThreadRelationshipBuilder[] builders = new PerThreadRelationshipBuilder[threads];
        for (int i = 0; i < queues.length; i++) {
            final ArrayBlockingQueue<RelationshipsBatch> queue = new ArrayBlockingQueue<>(PER_THREAD_IN_FLIGHT);
            final PerThreadRelationshipBuilder builder = new PerThreadRelationshipBuilder(
                    i, idBase, batchSize, queue, progress, idMap,
                    outOffsets, outAdjacency, inOffsets, inAdjacency);
            queues[i] = queue;
            builders[i] = builder;
            idBase += (long) batchSize;
        }

        final Collection<Future<?>> jobs =
                ParallelUtil.run(Arrays.asList(builders), false, threadPool, null);

        final RelationshipsScanner scanner = new RelationshipsScanner(api, setup, idMap, inFlight, queues, batchSize);
        scanner.run();
        ParallelUtil.awaitTermination(jobs);
    }
}
