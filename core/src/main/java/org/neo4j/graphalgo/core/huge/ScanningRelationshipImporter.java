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
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfIntArray;

final class ScanningRelationshipImporter {

    private static final int PER_THREAD_IN_FLIGHT = 1 << 7;

    private final GraphSetup setup;
    private final GraphDatabaseAPI api;
    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final HugeIdMap idMap;
    private final HugeWeightMapBuilder weights;
    private final HugeAdjacencyBuilder outAdjacency;
    private final HugeAdjacencyBuilder inAdjacency;
    private final ExecutorService threadPool;
    private final int concurrency;

    private ScanningRelationshipImporter(
            GraphSetup setup,
            GraphDatabaseAPI api,
            ImportProgress progress,
            AllocationTracker tracker,
            HugeIdMap idMap,
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        this.setup = setup;
        this.api = api;
        this.progress = progress;
        this.tracker = tracker;
        this.idMap = idMap;
        this.weights = weights;
        this.outAdjacency = outAdjacency;
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
            HugeWeightMapBuilder weights,
            HugeAdjacencyBuilder outAdjacency,
            HugeAdjacencyBuilder inAdjacency,
            ExecutorService threadPool,
            int concurrency) {
        if (!ParallelUtil.canRunInParallel(threadPool)) {
            // TODO: single thread impl
            return null;
        }
        return new ScanningRelationshipImporter(
                setup, api, progress, tracker, idMap, weights,
                outAdjacency, inAdjacency, threadPool, concurrency);
    }

    HugeWeightMapping run() {
        final ThreadSizing threadSizing = new ThreadSizing(concurrency, idMap.nodeCount(), threadPool);
        return run(threadSizing.numberOfThreads(), threadSizing.batchSize());
    }

    private HugeWeightMapping run(int threads, int batchSize) {
        ThreadLoader loader = new ThreadLoader(tracker, weights, outAdjacency, inAdjacency, threads);
        long idBase = 0L;
        int elementsForThread = (int) Math.min(batchSize, idMap.nodeCount() - idBase);
        while (elementsForThread > 0) {
            loader.add(api, progress, idBase, elementsForThread);
            idBase += (long) batchSize;
            elementsForThread = (int) Math.min(batchSize, idMap.nodeCount() - idBase);
        }
        loader.finish();

        final Collection<Future<?>> jobs =
                ParallelUtil.run(Arrays.asList(loader.builders), false, threadPool, null);

        int inFlight = threads * PER_THREAD_IN_FLIGHT;
        int baseQueueBatchSize = Math.max(1 << 4, Math.min(1 << 12, setup.batchSize));
        int queueBatchSize = 3 * baseQueueBatchSize;
        final RelationshipsScanner scanner = new RelationshipsScanner(
                api, setup, progress, tracker, idMap,
                inFlight, queueBatchSize, loader.queues, loader.outDegrees, loader.inDegrees, batchSize);
        scanner.run();
        ParallelUtil.awaitTermination(jobs);

        return loader.weights.build();
    }

    private static final class ThreadLoader {
        private final AllocationTracker tracker;
        private final HugeWeightMapBuilder weights;
        private final HugeAdjacencyBuilder outAdjacency;
        private final HugeAdjacencyBuilder inAdjacency;

        private ArrayBlockingQueue<RelationshipsBatch>[] queues;
        private PerThreadRelationshipBuilder[] builders;
        private int[][] outDegrees;
        private int[][] inDegrees;

        private int index;

        ThreadLoader(
                AllocationTracker tracker,
                HugeWeightMapBuilder weights,
                HugeAdjacencyBuilder outAdjacency,
                HugeAdjacencyBuilder inAdjacency,
                int threads) {
            this.tracker = tracker;
            this.weights = weights;
            this.outAdjacency = outAdjacency;
            this.inAdjacency = inAdjacency;
            weights.prepare(threads);
            //noinspection unchecked
            queues = new ArrayBlockingQueue[threads];
            builders = new PerThreadRelationshipBuilder[threads];
            if (outAdjacency != null) {
                outDegrees = new int[threads][];
            }
            if (inAdjacency != null) {
                inDegrees = new int[threads][];
            }
        }

        void add(
                GraphDatabaseAPI api,
                ImportProgress progress,
                long startId,
                int numberOfElements) {
            int index = this.index++;
            queues[index] = new ArrayBlockingQueue<>(PER_THREAD_IN_FLIGHT);
            int[] ins = null, outs = null;
            if (inDegrees != null) {
                tracker.add(sizeOfIntArray(numberOfElements));
                ins = inDegrees[index] = new int[numberOfElements];
            }
            if (outDegrees != null) {
                tracker.add(sizeOfIntArray(numberOfElements));
                outs = outDegrees[index] = new int[numberOfElements];
            }
            HugeWeightMapBuilder threadWeights = weights.threadLocalCopy(index, numberOfElements);
            builders[index] = new PerThreadRelationshipBuilder(
                    api, progress, tracker, threadWeights, queues[index],
                    index, startId, numberOfElements,
                    outs, outAdjacency, ins, inAdjacency);
        }

        void finish() {
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
        }
    }
}
