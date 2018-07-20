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

import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

final class ThreadSizing {

    // batch size is used to pre-size multiple arrays
    // 2B elements might even be too much as arrays need to be allocated with
    // a consecutive chunk of memory
    // possible idea: retry with lower batch sizes if alloc hits an OOM?
    private static final long MAX_BATCH_SIZE = 2_000_000_000L;

    private static final String NOT_ENOUGH_THREADS_AVAILABLE =
            "There are only %d threads available and with %d nodes this would mean that "
                    + "every thread would have to process %d nodes each, which is too large and unsupported.";

    private static final String TOO_MANY_THREADS_REQUIRED =
            "Importing %d nodes would need %d threads which cannot be created.";

    private final int totalThreads;
    private final int importerThreads;
    private final int importerBatchSize;

    ThreadSizing(int concurrency, long nodeCount, ExecutorService executor) {
        // we need at least that many threads, probably more
        long threadLowerBound = nodeCount / MAX_BATCH_SIZE;

        // start with the desired level of concurrency
        long targetThreads = (long) concurrency;

        // aim for 3:1 of importer:scanner threads
        long targetImporterThreads = targetThreads * 3L / 4L;

        // we batch by shifting on the node id, so the batchSize must be a power of 2
        long batchSize = BitUtil.nextHighestPowerOfTwo(ceilDiv(nodeCount, targetImporterThreads));

        // increase thread size until we have a small enough batch size
        while (batchSize > MAX_BATCH_SIZE) {
            targetImporterThreads = Math.max(threadLowerBound, 1L + targetImporterThreads);
            targetThreads = targetImporterThreads * 4L / 3L;
            batchSize = BitUtil.nextHighestPowerOfTwo(ceilDiv(nodeCount, targetImporterThreads));
        }

        // we need to run all threads at the same time and not have them queued
        // or else we risk a deadlock where the scanner waits on an importer thread to finish
        // but that importer thread is queued and hasn't even started
        // If we have another pool, we just have to hope for the best (or, maybe throw?)
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
            // TPE only increases to max threads if the queue is full, (see their JavaDoc)
            // but by that point it's already too late for us, so we have to fit every thread in the core pool
            long availableThreads = (long) (pool.getCorePoolSize() - pool.getActiveCount());
            if (availableThreads < targetThreads) {
                targetThreads = availableThreads;
                targetImporterThreads = targetThreads * 3L / 4L;
                batchSize = BitUtil.nextHighestPowerOfTwo(ceilDiv(nodeCount, targetImporterThreads));
                if (batchSize > MAX_BATCH_SIZE) {
                    throw new IllegalArgumentException(
                            String.format(NOT_ENOUGH_THREADS_AVAILABLE, availableThreads, nodeCount, batchSize)
                    );
                }
            }
        }

        if (targetThreads > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    String.format(TOO_MANY_THREADS_REQUIRED, nodeCount, targetThreads)
            );
        }

        // int casts are safe as both are < MAX_BATCH_SIZE
        this.totalThreads = (int) targetThreads;
        this.importerThreads = (int) targetImporterThreads;
        this.importerBatchSize = (int) batchSize;
    }

    int numberOfThreads() {
        return totalThreads;
    }

    int numberOfImporterThreads() {
        return importerThreads;
    }

    int importerBatchSize() {
        return importerBatchSize;
    }

    private static long ceilDiv(long dividend, long divisor) {
        return 1L + (-1L + dividend) / divisor;
    }
}
