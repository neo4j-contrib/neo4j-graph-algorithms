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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.concurrent.atomic.AtomicLong;

public final class ImportProgress {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long approxOperations;
    private final long progressMask;
    private final int relationProgressShift;

    private final AtomicLong nodeProgress;

    public ImportProgress(
            ProgressLogger progressLogger,
            AllocationTracker tracker,
            long nodeCount,
            long maxRelCount,
            boolean loadIncoming,
            boolean loadOutgoing) {
        this.progressLogger = progressLogger;
        this.tracker = tracker;
        this.nodeCount = nodeCount;
        long relOperations = (loadIncoming ? maxRelCount : 0) + (loadOutgoing ? maxRelCount : 0);
        long relFactor = nodeCount > 0 ? BitUtil.nearbyPowerOfTwo(relOperations / nodeCount) : 0;
        relationProgressShift = Long.numberOfTrailingZeros(relFactor);
        approxOperations = nodeCount + (nodeCount << relationProgressShift);
        progressMask = (BitUtil.nearbyPowerOfTwo(nodeCount) >>> 6) - 1;
        nodeProgress = new AtomicLong();
    }

    public void nodeProgress() {
        long nodes = nodeProgress.incrementAndGet();
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    nodes,
                    approxOperations,
                    tracker);
        }
    }

    public void relProgress() {
        long nodes = nodeProgress.incrementAndGet();
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    (nodes << relationProgressShift) + nodeCount,
                    approxOperations,
                    tracker);
        }
    }

    public void resetForRelationships() {
        nodeProgress.set(0);
    }
}
