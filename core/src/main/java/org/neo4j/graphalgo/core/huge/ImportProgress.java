package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.concurrent.atomic.AtomicLong;

final class ImportProgress {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long approxOperations;
    private final long progressMask;
    private final int relationProgressShift;

    private final AtomicLong nodeProgress;

    ImportProgress(
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
        long relFactor = BitUtil.nearbyPowerOfTwo(relOperations / nodeCount);
        relationProgressShift = Long.numberOfTrailingZeros(relFactor);
        approxOperations = nodeCount + (nodeCount << relationProgressShift);
        progressMask = (BitUtil.nearbyPowerOfTwo(nodeCount) >>> 6) - 1;
        nodeProgress = new AtomicLong();
    }

    void nodeProgress(long delta) {
        long nodes = nodeProgress.addAndGet(delta);
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    nodes,
                    approxOperations,
                    tracker);
        }
    }

    void relProgress(long delta) {
        long nodes = nodeProgress.addAndGet(delta);
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    (nodes << relationProgressShift) + nodeCount,
                    approxOperations,
                    tracker);
        }
    }

    void resetForRelationships() {
        nodeProgress.set(0);
    }
}
