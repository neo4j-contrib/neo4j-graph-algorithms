package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

final class ImportProgress {

    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long approxOperations;
    private final long progressMask;
    private final int relationProgressShift;

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
    }

    void nodeProgress(long nodes) {
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    nodes,
                    approxOperations,
                    () -> tracker.getUsageString("memory usage: "));
        }
    }

    void relProgress(long nodes) {
        if ((nodes & progressMask) == 0) {
            progressLogger.logProgress(
                    (nodes << relationProgressShift) + nodeCount,
                    approxOperations,
                    () -> tracker.getUsageString("memory usage: "));
        }
    }
}
