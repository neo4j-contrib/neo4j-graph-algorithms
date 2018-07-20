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

import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.concurrent.ExecutorService;

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

    private ThreadSizing(
            final int totalThreads,
            final int importerThreads,
            final int importerBatchSize) {
        this.totalThreads = totalThreads;
        this.importerThreads = importerThreads;
        this.importerBatchSize = importerBatchSize;
    }

    static ThreadSizing of(int concurrency, long nodeCount, ExecutorService executor) {
        long availableThreads = (long) ParallelUtil.availableThreads(executor);
        return determineBestThreadSize(nodeCount, availableThreads, (long) concurrency);
    }

    private static ThreadSizing determineBestThreadSize(long nodeCount, long availableThreads, long targetThreads) {
        // we need to run all threads at the same time and not have them queued
        // or else we risk a deadlock where the scanner waits on an importer thread to finish
        // but that importer thread is queued and hasn't even started
        if (targetThreads > availableThreads) {
            return determineBestThreadSize(
                    nodeCount,
                    availableThreads,
                    availableThreads
            );
        }

        // aim for 1:1 of importer:scanner threads
        long targetImporterThreads = targetThreads >> 1L;

        // we batch by shifting on the node id, so the batchSize must be a power of 2
        long batchSize = BitUtil.nextHighestPowerOfTwo(ceilDiv(nodeCount, targetImporterThreads));

        // increase thread size until we have a small enough batch size
        if (batchSize > MAX_BATCH_SIZE) {
            long newTargetThreads = Math.max(nodeCount / MAX_BATCH_SIZE, 1L + targetImporterThreads) << 1L;

            // to avoid an endless loop because we're reducing the threadSize again at the beginning of this method
            if (newTargetThreads > availableThreads) {
                throw new IllegalArgumentException(
                        String.format(NOT_ENOUGH_THREADS_AVAILABLE, availableThreads, nodeCount, batchSize)
                );
            }

            return determineBestThreadSize(
                    nodeCount,
                    availableThreads,
                    newTargetThreads
            );
        }

        if (targetThreads > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    String.format(TOO_MANY_THREADS_REQUIRED, nodeCount, targetThreads)
            );
        }

        // int casts are safe as all are < MAX_BATCH_SIZE
        return new ThreadSizing(
                (int) targetThreads,
                (int) targetImporterThreads,
                (int) batchSize
        );
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
