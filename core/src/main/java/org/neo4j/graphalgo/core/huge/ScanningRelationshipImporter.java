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

import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.impl.newapi.PartialCursors;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class ScanningRelationshipImporter {

    static Runnable create(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            boolean loadDegrees,
            int concurrency) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            return new SerialScanning(
                    setup, api, progress, tracker, idMap, weights,
                    outAdjacency, inAdjacency, loadDegrees);
        }
        return new ParallelScanning(
                setup, api, dimensions, progress, tracker, idMap, weights, loadDegrees,
                outAdjacency, inAdjacency, threadPool, concurrency);
    }

    private ScanningRelationshipImporter() {}
}

final class ParallelScanning implements Runnable {

    private static final int PER_THREAD_IN_FLIGHT = 1 << 7;

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final GraphDimensions dimensions;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final boolean loadDegrees;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    ParallelScanning(
            GraphSetup setup,
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            boolean loadDegrees,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        this.setup = setup;
        this.api = api;
        this.dimensions = dimensions;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.loadDegrees = loadDegrees;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    @Override
    public void run() {
        run(ThreadSizing.of(concurrency, idMap.nodeCount(), threadPool));
    }

    private void run(ThreadSizing threadSizing) {
        int batchSize = threadSizing.importerBatchSize();
        int threads = threadSizing.numberOfImporterThreads();
        weights.prepare(threads, batchSize);
        ThreadLoader loader = new ThreadLoader(tracker, weights, outAdjacency, inAdjacency, loadDegrees, threads);
        long idBase = 0L;
        int elementsForThread = (int) Math.min(batchSize, idMap.nodeCount() - idBase);
        while (elementsForThread > 0) {
            loader.add(api, progress, idBase, elementsForThread);
            idBase += (long) batchSize;
            elementsForThread = (int) Math.min(batchSize, idMap.nodeCount() - idBase);
        }
        threads = loader.finish(batchSize);

        final Collection<Future<?>> jobs =
                ParallelUtil.run(Arrays.asList(loader.builders), false, threadPool, null);

        int inFlight = threads * PER_THREAD_IN_FLIGHT;
        int baseQueueBatchSize = Math.max(1 << 4, Math.min(1 << 12, setup.batchSize));
        int queueBatchSize = 3 * baseQueueBatchSize;

        QueueingScanner.Creator creator = QueueingScanner.of(
                api, progress, idMap, loader.outDegrees, loader.inDegrees, loadDegrees, queueBatchSize,
                setup, inFlight, weights.loadsWeights(), loader.queues, batchSize);

        Collection<Future<?>> scannerFutures = null;
        int scanners = threadSizing.numberOfThreads() - threads;
        if (scanners > 0) {
            long[] ids = PartialCursors.splitIdIntoPartialSegments(dimensions.allRelsCount(), scanners);
            Collection<Runnable> scannerTasks = new ArrayList<>(scanners);
            for (int i = 0; i < scanners; i++) {
                long start = ids[i], end = ids[i + 1];
                RelationshipsScanner scanner = creator.ofRange(start, end, dimensions.relationshipTypeId());
                scannerTasks.add(scanner);
            }
            scannerFutures = ParallelUtil.run(scannerTasks, true, threadPool, null);
        } else {
            final RelationshipsScanner scanner = creator.ofAll();
            scanner.run();
        }

        if (scannerFutures != null) {
            ParallelUtil.awaitTermination(scannerFutures);
            QueueingScanner.sendSentinelToImportThreads(loader.queues);
        }
        ParallelUtil.awaitTermination(jobs);
    }

    private static final class ThreadLoader {
        private final boolean loadDegrees;
        private final AllocationTracker tracker;
        private final HugeWeightMapBuilder weights;
        private final HugeAdjacencyBuilder outAdjacency;
        private final HugeAdjacencyBuilder inAdjacency;

        private ArrayBlockingQueue<RelationshipsBatch>[] queues;
        private PerThreadRelationshipBuilder[] builders;
        private long[][] outDegrees;
        private long[][] inDegrees;

        private int index;

        ThreadLoader(
                AllocationTracker tracker,
                HugeWeightMapBuilder weights,
                HugeAdjacencyBuilder outAdjacency,
                HugeAdjacencyBuilder inAdjacency,
                boolean loadDegrees,
                int threads) {
            this.tracker = tracker;
            this.weights = weights;
            this.outAdjacency = outAdjacency;
            this.inAdjacency = inAdjacency;
            this.loadDegrees = loadDegrees;
            //noinspection unchecked
            queues = new ArrayBlockingQueue[threads];
            builders = new PerThreadRelationshipBuilder[threads];
            if (outAdjacency != null) {
                outDegrees = new long[threads][];
                tracker.add(sizeOfObjectArray(threads));
            }
            if (inAdjacency != null) {
                inDegrees = new long[threads][];
                tracker.add(sizeOfObjectArray(threads));
            }
        }

        void add(
                GraphDatabaseAPI api,
                ImportProgress progress,
                long startId,
                int numberOfElements) {
            int index = this.index++;
            queues[index] = new ArrayBlockingQueue<>(PER_THREAD_IN_FLIGHT);
            HugeAdjacencyBuilder outAB = null, inAB = null;
            if (outAdjacency != null && outDegrees != null) {
                long[] degrees = outDegrees[index] = new long[numberOfElements];
                outAB = outAdjacency.threadLocalCopy(degrees, loadDegrees);
                tracker.add(sizeOfLongArray(numberOfElements));
            }
            if (inAdjacency != null && inDegrees != null) {
                long[] degrees = inDegrees[index] = new long[numberOfElements];
                inAB = inAdjacency.threadLocalCopy(degrees, loadDegrees);
                tracker.add(sizeOfLongArray(numberOfElements));
            }
            HugeWeightMapBuilder threadWeights = weights.threadLocalCopy(index, numberOfElements);
            builders[index] = new PerThreadRelationshipBuilder(
                    api, progress, tracker, threadWeights, queues[index],
                    index, startId, numberOfElements, outAB, inAB);
        }

        int finish(int perThreadSize) {
            int length = index;
            weights.finish(length);
            if (length < queues.length) {
                queues = Arrays.copyOf(queues, length);
                builders = Arrays.copyOf(builders, length);
                if (inDegrees != null) {
                    inDegrees = Arrays.copyOf(inDegrees, length);
                }
                if (outDegrees != null) {
                    outDegrees = Arrays.copyOf(outDegrees, length);
                }
            }
            if (outAdjacency != null && outDegrees != null) {
                outAdjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(outDegrees, perThreadSize));
            }
            if (inAdjacency != null && inDegrees != null) {
                inAdjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(inDegrees, perThreadSize));
            }
            return length;
        }
    }
}

final class SerialScanning implements Runnable {

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final int nodeCount;
    private final boolean loadDegrees;

    SerialScanning(
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            boolean loadDegrees) {
        this.loadDegrees = loadDegrees;
        if (idMap.nodeCount() > Integer.MAX_VALUE) {
            failForTooMuchNodes(idMap.nodeCount(), null);
        }
        this.nodeCount = (int) idMap.nodeCount();
        this.setup = setup;
        this.api = api;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.outAdjacency = outAdjacency;
        this.inAdjacency = inAdjacency;
    }

    @Override
    public void run() {
        weights.prepare(1, 0);
        long[][] outDegrees, inDegrees;
        final PerThreadRelationshipBuilder builder;
        try {
            HugeWeightMapBuilder threadWeights = weights.threadLocalCopy(0, nodeCount);
            outDegrees = createDegreesBuffer(nodeCount, tracker, outAdjacency);
            inDegrees = createDegreesBuffer(nodeCount, tracker, inAdjacency);
            builder = new PerThreadRelationshipBuilder(
                    api, progress, tracker, threadWeights, null, 0, 0L, nodeCount,
                    HugeAdjacencyBuilder.threadLocal(outAdjacency, degrees(outDegrees), loadDegrees),
                    HugeAdjacencyBuilder.threadLocal(inAdjacency, degrees(inDegrees), loadDegrees));
        } catch (OutOfMemoryError oom) {
            failForTooMuchNodes(idMap.nodeCount(), oom);
            return;
        }

        int baseQueueBatchSize = Math.max(1 << 4, Math.min(1 << 12, setup.batchSize));
        int queueBatchSize = 3 * baseQueueBatchSize;

        final RelationshipsScanner scanner = new NonQueueingScanner(
                api, progress, idMap, outDegrees, inDegrees, loadDegrees,
                queueBatchSize, setup, weights.loadsWeights(), builder);
        scanner.run();
    }

    private static long[][] createDegreesBuffer(
            final int nodeCount,
            final AllocationTracker tracker,
            final HugeAdjacencyBuilder adjacency) {
        if (adjacency != null) {
            long[][] degrees = new long[1][nodeCount];
            tracker.add(sizeOfLongArray(nodeCount) + sizeOfObjectArray(1));
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, 0));
            return degrees;
        }
        return null;
    }

    private static long[] degrees(long[][] degrees) {
        return degrees != null ? degrees[0] : null;
    }

    private static void failForTooMuchNodes(long nodeCount, Throwable cause) {
        String msg = String.format(
                "Cannot import %d nodes in a single thread, you have to provide a valid thread pool",
                nodeCount
        );
        throw new IllegalArgumentException(msg, cause);
    }
}
