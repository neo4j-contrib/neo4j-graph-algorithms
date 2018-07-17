package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Read;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class WeightBuilder {

    abstract void addWeightImporter(int pageIndex);

    abstract void finish();

    abstract void addWeight(
            CursorFactory cursors,
            Read read,
            long relationshipReference,
            long propertiesReference,
            long sourceNodeId,
            long targetNodeId);

    static WeightBuilder of(
            HugeWeightMapBuilder weights,
            int numPages,
            int pageSize,
            AllocationTracker tracker) {
        if (!weights.loadsWeights()) {
            return NoWeights.INSTANCE;
        }
        weights.prepare(numPages, pageSize);

        tracker.add(sizeOfObjectArray(numPages));
        HugeWeightMapBuilder[] builders = new HugeWeightMapBuilder[numPages];
        return new PagedWeights(weights, builders, pageSize);
    }

    private static final class PagedWeights extends WeightBuilder {

        private final HugeWeightMapBuilder weights;
        private final HugeWeightMapBuilder[] builders;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;

        private PagedWeights(
                HugeWeightMapBuilder weights,
                HugeWeightMapBuilder[] builders,
                int pageSize) {
            this.weights = weights;
            this.builders = builders;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
        }

        void addWeightImporter(int pageIndex) {
            builders[pageIndex] = weights.threadLocalCopy(pageIndex, pageSize);
        }

        @Override
        void finish() {
            weights.finish(builders.length);
        }

        @Override
        void addWeight(
                final CursorFactory cursors,
                final Read read,
                final long relationshipReference,
                final long propertiesReference,
                final long sourceNodeId,
                final long targetNodeId) {
            int pageIdx = (int) (sourceNodeId >>> pageShift);
            int localId = (int) (sourceNodeId & pageMask);
            builders[pageIdx].load(
                    relationshipReference,
                    propertiesReference,
                    targetNodeId,
                    localId,
                    cursors,
                    read
            );
        }
    }

    private static final class NoWeights extends WeightBuilder {

        private static final WeightBuilder INSTANCE = new NoWeights();

        @Override
        void addWeightImporter(int pageIndex) {
        }

        @Override
        void finish() {
        }

        @Override
        void addWeight(
                final CursorFactory cursors,
                final Read read,
                final long relationshipReference,
                final long propertiesReference,
                final long sourceNodeId,
                final long targetNodeId) {
        }
    }
}
