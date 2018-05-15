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
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

final class ScanningRelationshipImporter {

    private static final long MAX_BATCH_SIZE = 2_000_000_000L;
    private static final int PER_THREAD_IN_FLIGHT = 1 << 4;

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeLongArray outOffsets;
    private final HugeAdjacencyListBuilder outAdjacency;
    private final HugeLongArray inOffsets;
    private final HugeAdjacencyListBuilder inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    private ScanningRelationshipImporter(
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeLongArray outOffsets,
            HugeAdjacencyListBuilder outAdjacency,
            HugeLongArray inOffsets,
            HugeAdjacencyListBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        this.setup = setup;
        this.api = api;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.outOffsets = outOffsets;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.inAdjacency = inAdjacency;
        this.threadPool = threadPool;
        this.concurrency = concurrency;
    }

    static ScanningRelationshipImporter create(
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeLongArray outOffsets,
            HugeAdjacencyListBuilder outAdjacency,
            HugeLongArray inOffsets,
            HugeAdjacencyListBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            // TODO: single thread impl
            return null;
        }
        return new ScanningRelationshipImporter(
                setup, api, progress, tracker, idMap,
                outOffsets, outAdjacency, inOffsets, inAdjacency,
                threadPool, concurrency);
    }

    void run() {
        long targetThreads = (long) BitUtil.nextHighestPowerOfTwo(concurrency);
        long batchSize = BitUtil.nextHighestPowerOfTwo(ParallelUtil.threadSize(targetThreads, idMap.nodeCount()));
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
//        noinspection unchecked
        ArrayBlockingQueue<RelationshipsBatch>[] queues = new ArrayBlockingQueue[threads];
        PerThreadRelationshipBuilder[] builders = new PerThreadRelationshipBuilder[threads];
        int[][] outDegrees = allocateDegrees(outOffsets != null, threads);
        int[][] inDegrees = allocateDegrees(inOffsets != null, threads);
        int i = 0;
        for (; i < threads; i++) {
            int elementsForThread = (int) Math.min(batchSize, idMap.nodeCount() - idBase);
            if (elementsForThread <= 0) {
                break;
            }
            final ArrayBlockingQueue<RelationshipsBatch> queue = new ArrayBlockingQueue<>(PER_THREAD_IN_FLIGHT);
            int[] out = allocateDegree(outDegrees, elementsForThread, i);
            int[] in = allocateDegree(inDegrees, elementsForThread, i);
            final PerThreadRelationshipBuilder builder = new PerThreadRelationshipBuilder(
                    progress, tracker, i, idBase, elementsForThread, queue,
                    out, outOffsets, outAdjacency, in, inOffsets, inAdjacency);

            queues[i] = queue;
            builders[i] = builder;
            idBase += (long) batchSize;
        }
        if (i < threads) {
            builders = Arrays.copyOf(builders, i);
            queues = Arrays.copyOf(queues, i);
        }

        final Collection<Future<?>> jobs =
                ParallelUtil.run(Arrays.asList(builders), false, threadPool, null);

        int queueBatchSize = (int) BitUtil.align((long) Math.max(1 << 13, setup.batchSize), 2);
        final RelationshipsScanner scanner = new RelationshipsScanner(
                api, setup, progress, tracker, idMap,
                inFlight, queueBatchSize, queues, outDegrees, inDegrees, batchSize);
        scanner.run();
        ParallelUtil.awaitTermination(jobs);
    }

    private int[][] allocateDegrees(boolean shouldAllocate, int size) {
        if (shouldAllocate) {
            tracker.add(sizeOfObjectArray(size));
            return new int[size][];
        }
        return null;
    }

    private int[] allocateDegree(int[][] into, int size, int index) {
        if (into != null) {
            tracker.add(sizeOfIntArray(size));
            return into[index] = new int[size];
        }
        return null;
    }
}
