package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

abstract class AdjacencyBuilder {

    abstract void addAdjacencyImporter(
            AllocationTracker tracker,
            boolean loadDegrees,
            int pageIndex);

    abstract int add(
            VisitRelationship visit,
            long nodeId);

    abstract void finish();

    static AdjacencyBuilder of(
            HugeAdjacencyBuilder adjacency,
            int numPages,
            int pageSize,
            AllocationTracker tracker) {
        if (adjacency == null) {
            return NoAdjacency.INSTANCE;
        }
        tracker.add(sizeOfObjectArray(numPages) << 1);
        HugeAdjacencyBuilder[] builders = new HugeAdjacencyBuilder[numPages];
        long[][] degrees = new long[numPages][];
        return new PagedAdjacency(adjacency, builders, degrees, pageSize);
    }

    private static final class PagedAdjacency extends AdjacencyBuilder {

        private final HugeAdjacencyBuilder adjacency;
        private final HugeAdjacencyBuilder[] builders;
        private final long[][] degrees;
        private final int pageSize;
        private final int pageShift;
        private final long pageMask;

        private PagedAdjacency(
                HugeAdjacencyBuilder adjacency,
                HugeAdjacencyBuilder[] builders,
                long[][] degrees,
                int pageSize) {
            this.adjacency = adjacency;
            this.builders = builders;
            this.degrees = degrees;
            this.pageSize = pageSize;
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
        }

        @Override
        void addAdjacencyImporter(AllocationTracker tracker, boolean loadDegrees, int pageIndex) {
            tracker.add(sizeOfLongArray(pageSize));
            long[] offsets = degrees[pageIndex] = new long[pageSize];
            builders[pageIndex] = adjacency.threadLocalCopy(offsets, loadDegrees);
            builders[pageIndex].prepare();
        }

        @Override
        void finish() {
            adjacency.setGlobalOffsets(HugeAdjacencyOffsets.of(degrees, pageSize));
        }

        @Override
        int add(VisitRelationship visit, long nodeId) {
            return visit.flush(
                    builders[(int) (nodeId >>> pageShift)],
                    (int) (nodeId & pageMask)
            );
        }
    }

    private static final class NoAdjacency extends AdjacencyBuilder {

        private static final AdjacencyBuilder INSTANCE = new NoAdjacency();

        @Override
        void addAdjacencyImporter(
                AllocationTracker tracker,
                boolean loadDegrees,
                int pageIndex) {
        }

        @Override
        void finish() {
        }

        @Override
        int add(VisitRelationship visit, long nodeId) {
            return 0;
        }
    }
}
