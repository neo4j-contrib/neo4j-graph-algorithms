package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

final class HugeIdMapBuilder {

    static HugeIdMap build(
            HugeLongArrayBuilder idMapBuilder,
            long highestNodeId,
            AllocationTracker tracker) {
        HugeLongArray graphIds = idMapBuilder.build();
        SparseLongArray nodeToGraphIds = SparseLongArray.newArray(highestNodeId, tracker);

        try (HugeLongArray.Cursor cursor = graphIds.cursor(graphIds.newCursor())) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                long internalId = cursor.base + offset;
                for (int i = offset; i < limit; ++i, ++internalId) {
                    nodeToGraphIds.set(array[i], internalId);
                }
            }
        }

        return new HugeIdMap(graphIds, nodeToGraphIds, idMapBuilder.size());
    }

    private HugeIdMapBuilder() {
    }
}
